use futures::{SinkExt, StreamExt};
use log::*;
use omnipaxos::{messages::{leader_election::{HBRequest, IthacaElectionMessage}, sequence_paxos::MessageAction}, OmniPaxos};
use omnipaxos_kv::common::{
    kv::{ClientId, NodeId},
    messages::*,
    utils::*,
};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{collections::HashMap, str::FromStr};
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::Receiver,
};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::configs::OmniPaxosKVConfig;

pub struct Network {
    peers: Vec<NodeId>,
    peer_connections: Vec<Option<PeerConnection>>,
    client_connections: HashMap<ClientId, ClientConnection>,
    max_client_id: Arc<Mutex<ClientId>>,
    batch_size: usize,
    client_message_sender: Sender<(ClientId, ClientMessage)>,
    cluster_message_sender: Sender<(NodeId, ClusterMessage)>,
    pub cluster_messages: Receiver<(NodeId, ClusterMessage)>,
    pub client_messages: Receiver<(ClientId, ClientMessage)>,
}

fn get_addrs(config: OmniPaxosKVConfig) -> (SocketAddr, Vec<SocketAddr>) {
    let listen_address_str = format!(
        "{}:{}",
        config.local.listen_address, config.local.listen_port
    );
    let listen_address = SocketAddr::from_str(&listen_address_str).expect(&format!(
        "{listen_address_str} is an invalid listen address"
    ));
    let node_addresses: Vec<SocketAddr> = config
        .cluster
        .node_addrs
        .into_iter()
        .map(|addr_str| match addr_str.to_socket_addrs() {
            Ok(mut addrs) => addrs.next().unwrap(),
            Err(e) => panic!("Address {addr_str} is invalid: {e}"),
        })
        .collect();
    (listen_address, node_addresses)
}

impl Network {
    // Creates a new network with connections other server nodes in the cluster and any clients.
    // Waits until connections to all servers and clients are established before resolving.
    pub async fn new(config: OmniPaxosKVConfig, batch_size: usize) -> Self {
        let (listen_address, node_addresses) = get_addrs(config.clone());
        let id = config.local.server_id;
        let peer_addresses: Vec<(NodeId, SocketAddr)> = config
            .cluster
            .nodes
            .into_iter()
            .zip(node_addresses.into_iter())
            .filter(|(node_id, _addr)| *node_id != id)
            .collect();
        let cluster_connections: Vec<Option<PeerConnection>> = (0..peer_addresses.len())
            .map(|_| Default::default())
            .collect();
        //cluster_connections.resize_with(peer_addresses.len(), Default::default);
        let (cluster_message_sender, cluster_messages) = tokio::sync::mpsc::channel(batch_size);
        let (client_message_sender, client_messages) = tokio::sync::mpsc::channel(batch_size);
        let mut network = Self {
            peers: peer_addresses.iter().map(|(id, _)| *id).collect(),
            peer_connections: cluster_connections,
            client_connections: HashMap::with_capacity(10000),
            max_client_id: Arc::new(Mutex::new(0)),
            batch_size,
            client_message_sender,
            cluster_message_sender,
            cluster_messages,
            client_messages,
        };
        let num_clients = config.local.num_clients;
        network
            .initialize_connections(id, num_clients, peer_addresses, listen_address)
            .await;
        network
    }

    async fn initialize_connections(
        &mut self,
        id: NodeId,
        num_clients: usize,
        peers: Vec<(NodeId, SocketAddr)>,
        listen_address: SocketAddr,
    ) {
        let (connection_sink, mut connection_source) = mpsc::channel(30);
        let listener_handle =
            self.spawn_connection_listener(connection_sink.clone(), listen_address);
        self.spawn_peer_connectors(connection_sink.clone(), id, peers);
        while let Some(new_connection) = connection_source.recv().await {
            match new_connection {
                NewConnection::ToPeer(connection) => {
                    let peer_idx = self.cluster_id_to_idx(connection.peer_id).unwrap();
                    self.peer_connections[peer_idx] = Some(connection);
                }
                NewConnection::ToClient(connection) => {
                    let _ = self
                        .client_connections
                        .insert(connection.client_id, connection);
                }
            }
            let all_clients_connected = self.client_connections.len() >= num_clients;
            let all_cluster_connected = self.peer_connections.iter().all(|c| c.is_some());
            if all_clients_connected && all_cluster_connected {
                listener_handle.abort();
                break;
            }
        }
    }

    fn spawn_connection_listener(
        &self,
        connection_sender: Sender<NewConnection>,
        listen_address: SocketAddr,
    ) -> tokio::task::JoinHandle<()> {
        let client_sender = self.client_message_sender.clone();
        let cluster_sender = self.cluster_message_sender.clone();
        let max_client_id_handle = self.max_client_id.clone();
        let batch_size = self.batch_size;
        tokio::spawn(async move {
            let listener = TcpListener::bind(listen_address).await.unwrap();
            loop {
                match listener.accept().await {
                    Ok((tcp_stream, socket_addr)) => {
                        info!("New connection from {socket_addr}");
                        tcp_stream.set_nodelay(true).unwrap();
                        tokio::spawn(Self::handle_incoming_connection(
                            tcp_stream,
                            client_sender.clone(),
                            cluster_sender.clone(),
                            connection_sender.clone(),
                            max_client_id_handle.clone(),
                            batch_size,
                        ));
                    }
                    Err(e) => error!("Error listening for new connection: {:?}", e),
                }
            }
        })
    }

    async fn handle_incoming_connection(
        connection: TcpStream,
        client_message_sender: Sender<(ClientId, ClientMessage)>,
        cluster_message_sender: Sender<(NodeId, ClusterMessage)>,
        connection_sender: Sender<NewConnection>,
        max_client_id_handle: Arc<Mutex<ClientId>>,
        batch_size: usize,
    ) {
        // Identify connector's ID and type by handshake
        let mut registration_connection = frame_registration_connection(connection);
        let registration_message = registration_connection.next().await;
        let new_connection = match registration_message {
            Some(Ok(RegistrationMessage::NodeRegister(node_id))) => {
                info!("Identified connection from node {node_id}");
                let underlying_stream = registration_connection.into_inner().into_inner();
                NewConnection::ToPeer(PeerConnection::new(
                    node_id,
                    underlying_stream,
                    batch_size,
                    cluster_message_sender,
                ))
            }
            Some(Ok(RegistrationMessage::ClientRegister)) => {
                let next_client_id = {
                    let mut max_client_id = max_client_id_handle.lock().unwrap();
                    *max_client_id += 1;
                    *max_client_id
                };
                info!("Identified connection from client {next_client_id}");
                let underlying_stream = registration_connection.into_inner().into_inner();
                NewConnection::ToClient(ClientConnection::new(
                    next_client_id,
                    underlying_stream,
                    batch_size,
                    client_message_sender,
                ))
            }
            Some(Err(err)) => {
                error!("Error deserializing handshake: {:?}", err);
                return;
            }
            None => {
                info!("Connection to unidentified source dropped");
                return;
            }
        };
        connection_sender.send(new_connection).await.unwrap();
    }

    fn spawn_peer_connectors(
        &self,
        connection_sender: Sender<NewConnection>,
        my_id: NodeId,
        peers: Vec<(NodeId, SocketAddr)>,
    ) {
        let peers_to_connect_to = peers.into_iter().filter(|(peer_id, _)| *peer_id < my_id);
        for (peer, peer_address) in peers_to_connect_to {
            let reconnect_delay = Duration::from_secs(1);
            let mut reconnect_interval = tokio::time::interval(reconnect_delay);
            let cluster_sender = self.cluster_message_sender.clone();
            let connection_sender = connection_sender.clone();
            let batch_size = self.batch_size;
            tokio::spawn(async move {
                // Establish connection
                let peer_connection = loop {
                    reconnect_interval.tick().await;
                    match TcpStream::connect(peer_address).await {
                        Ok(connection) => {
                            info!("New connection to node {peer}");
                            connection.set_nodelay(true).unwrap();
                            break connection;
                        }
                        Err(err) => {
                            error!("Establishing connection to node {peer} failed: {err}")
                        }
                    }
                };
                // Send handshake
                let mut registration_connection = frame_registration_connection(peer_connection);
                let handshake = RegistrationMessage::NodeRegister(my_id);
                if let Err(err) = registration_connection.send(handshake).await {
                    error!("Error sending handshake to {peer}: {err}");
                    return;
                }
                let underlying_stream = registration_connection.into_inner().into_inner();
                // Create connection actor
                let peer_actor =
                    PeerConnection::new(peer, underlying_stream, batch_size, cluster_sender);
                let new_connection = NewConnection::ToPeer(peer_actor);
                connection_sender.send(new_connection).await.unwrap();
            });
        }
    }

    pub fn send_to_cluster(&mut self, to: NodeId, msg: ClusterMessage) {
        match self.cluster_id_to_idx(to) {
            Some(idx) => match &mut self.peer_connections[idx] {
                Some(ref mut connection) => {
                    if connection.is_active {
                        if let Err(err) = connection.send(msg) {
                            warn!("Couldn't send msg to peer {to}: {err}");
                            self.peer_connections[idx] = None;
                        }
                    }
                }
                None => warn!("Not connected to node {to}"),
            },
            None => error!("Sending to unexpected node {to}"),
        }
    }

    pub fn send_le_to_cluster(&mut self, to: NodeId, msg: ClusterMessage) {
        match self.cluster_id_to_idx(to) {
            Some(idx) => match &mut self.peer_connections[idx] {
                Some(ref mut connection) => {
                    if connection.is_active {
                        if let Err(err) = connection.send_le(msg) {
                            warn!("Couldn't send le msg to peer {to}: {err}");
                            self.peer_connections[idx] = None;
                        }
                    }
                }
                None => warn!("Not connected to node {to}"),
            },
            None => error!("Sending to unexpected node {to}"),
        }
    }

    pub fn send_to_client(&mut self, to: ClientId, msg: ServerMessage) {
        match self.client_connections.get_mut(&to) {
            Some(connection) => {
                if connection.is_active {
                    if let Err(err) = connection.send(msg) {
                        warn!("Couldn't send msg to client {to}: {err}");
                        self.client_connections.remove(&to);
                    }
                }
            }
            None => warn!("Not connected to client {to}"),
        }
    }

    // Removes all client and peer connections and ends their corresponding tasks.
    #[allow(dead_code)]
    pub fn shutdown(&mut self) {
        for (_, client_connection) in self.client_connections.drain() {
            client_connection.close();
        }
        for peer_connection in self.peer_connections.iter_mut() {
            if let Some(connection) = peer_connection.take() {
                connection.close();
            }
        }
    }
      
    // Removes all peer connections
    pub fn disconnect(&mut self) {
        for peer_connection in &mut self.peer_connections {
            if let Some(ref mut connection) = peer_connection {
                connection.is_active = false;
            }
        }
        for client_connection in &mut self.client_connections {
            client_connection.1.is_active = false;
        }
    }

    pub fn connect(&mut self) {
        for peer_connection in &mut self.peer_connections {
            if let Some(ref mut connection) = peer_connection {
                connection.is_active = true;
            }
        }
        for client_connection in &mut self.client_connections {
            client_connection.1.is_active = true;
        }
    }


    // Kills the connection to a specific node
    pub fn kill_link(&mut self, to: NodeId, server_id: NodeId) {
        match self.cluster_id_to_idx(to) {
            Some(idx) => {
                if let Some(connection) = &mut self.peer_connections[idx] {
                    connection.is_active = false;
                } else {
                    warn!("{server_id}: Not connected to node {to}");
                }
            }
            None => error!("Sending to unexpected node {to}"),
        }
    }
    

    pub fn connect_link(&mut self, to: NodeId, server_id: NodeId) {
        match self.cluster_id_to_idx(to) {
            Some(idx) => {
                if let Some(connection) = &mut self.peer_connections[idx] {
                    connection.is_active = true;
                } else {
                    warn!("{server_id}: Not connected to node {to}");
                }
            }
            None => error!("Sending to unexpected node {to}"),
        }
    }

    #[inline]
    fn cluster_id_to_idx(&self, id: NodeId) -> Option<usize> {
        self.peers.iter().position(|&p| p == id)
    }
}

enum NewConnection {
    ToPeer(PeerConnection),
    ToClient(ClientConnection),
}

struct PeerConnection {
    peer_id: NodeId,
    reader_task: JoinHandle<()>,
    writer_task: JoinHandle<()>,
    outgoing_messages: UnboundedSender<ClusterMessage>,
    le_messages: UnboundedSender<ClusterMessage>,
    is_active: bool,
}

impl PeerConnection {
    pub fn new(
        peer_id: NodeId,
        connection: TcpStream,
        batch_size: usize,
        incoming_messages: Sender<(NodeId, ClusterMessage)>,
    ) -> Self {
        let (reader, mut writer) = frame_cluster_connection(connection);
        // Reader Actor
        let reader_task = tokio::spawn(async move {
            let mut buf_reader = reader.ready_chunks(batch_size);
            while let Some(messages) = buf_reader.next().await {
                for msg in messages {
                    match msg {
                        Ok(m) => {
                            if let Err(_) = incoming_messages.send((peer_id, m)).await {
                                break;
                            };
                        }
                        Err(err) => {
                            error!("Error deserializing message: {:?}", err);
                        }
                    }
                }
            }
        });
        // Writer Actor
        let (message_tx, mut message_rx): (mpsc::UnboundedSender<ClusterMessage>, mpsc::UnboundedReceiver<ClusterMessage>) = mpsc::unbounded_channel();
        let (le_tx, mut le_rx) = mpsc::unbounded_channel();
        let writer_task = tokio::spawn(async move {
            let mut le_buffer = Vec::with_capacity(batch_size);
            let mut buffer = Vec::with_capacity(batch_size);
            loop {
                tokio::select! {
                    _ = le_rx.recv_many(&mut le_buffer, batch_size), if !le_rx.is_closed() => {
                        for msg in le_buffer.drain(..) {
                            if let Err(err) = writer.feed(msg).await {
                                error!("Couldn't send LE message to node {peer_id}: {err}");
                                return;
                            }
                        }
                        if let Err(err) = writer.flush().await {
                            error!("Couldn't flush LE messages to node {peer_id}: {err}");
                            return;
                        }
                    },
                    _ = message_rx.recv_many(&mut buffer, batch_size), if !message_rx.is_closed() => {
                        for msg in buffer.drain(..) {
                            if let Err(err) = writer.feed(msg.clone()).await {
                                error!("Couldn't send Paxos message to node {peer_id}: {err}");
                                return;
                            }
                        }
                        if let Err(err) = writer.flush().await {
                            error!("Couldn't flush Paxos messages to node {peer_id}: {err}");
                            return;
                        }
                    }
                }
        
                if le_rx.is_closed() && message_rx.is_closed() {
                    info!("Both LE and Paxos channels closed for node {peer_id}");
                    break;
                }
            }
            info!("Connection to node {peer_id} closed");
        });
        PeerConnection {
            peer_id,
            reader_task,
            writer_task,
            outgoing_messages: message_tx,
            le_messages: le_tx,
            is_active: true,
        }
    }

    pub fn send(
        &mut self,
        msg: ClusterMessage,
    ) -> Result<(), mpsc::error::SendError<ClusterMessage>> {
        self.outgoing_messages.send(msg)
    }

    pub fn send_le(&mut self, le_msg:ClusterMessage) -> Result<(), mpsc::error::SendError<ClusterMessage>> {
        self.le_messages.send(le_msg)
    }

    fn close(self) {
        self.reader_task.abort();
        self.writer_task.abort();
    }
}

struct ClientConnection {
    client_id: ClientId,
    reader_task: JoinHandle<()>,
    writer_task: JoinHandle<()>,
    outgoing_messages: UnboundedSender<ServerMessage>,
    is_active: bool,
}

impl ClientConnection {
    pub fn new(
        client_id: ClientId,
        connection: TcpStream,
        batch_size: usize,
        incoming_messages: Sender<(ClientId, ClientMessage)>,
    ) -> Self {
        let (reader, mut writer) = frame_servers_connection(connection);
        // Reader Actor
        let reader_task = tokio::spawn(async move {
            let mut buf_reader = reader.ready_chunks(batch_size);
            while let Some(messages) = buf_reader.next().await {
                for msg in messages {
                    match msg {
                        Ok(m) => incoming_messages.send((client_id, m)).await.unwrap(),
                        Err(err) => error!("Error deserializing message: {:?}", err),
                    }
                }
            }
        });
        // Writer Actor
        let (message_tx, mut message_rx) = mpsc::unbounded_channel();
        let writer_task = tokio::spawn(async move {
            let mut buffer = Vec::with_capacity(batch_size);
            while message_rx.recv_many(&mut buffer, batch_size).await != 0 {
                for msg in buffer.drain(..) {
                    if let Err(err) = writer.feed(msg).await {
                        error!("Couldn't send message to client {client_id}: {err}");
                        error!("Killing connection to client {client_id}");
                        return;
                    }
                }
                if let Err(err) = writer.flush().await {
                    error!("Couldn't send message to client {client_id}: {err}");
                    error!("Killing connection to client {client_id}");
                    return;
                }
            }
        });
        ClientConnection {
            client_id,
            reader_task,
            writer_task,
            outgoing_messages: message_tx,
            is_active: true,
        }
    }

    pub fn send(
        &mut self,
        msg: ServerMessage,
    ) -> Result<(), mpsc::error::SendError<ServerMessage>> {
        self.outgoing_messages.send(msg)
    }

    fn close(self) {
        self.reader_task.abort();
        self.writer_task.abort();
    }
}
