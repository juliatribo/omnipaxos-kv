use crate::{configs::ClientConfig, data_collection::ClientData, network::Network};
use chrono::Utc;
use log::*;
use omnipaxos_kv::common::{kv::*, messages::*};
use rand::Rng;
use std::time::Duration;
use tokio::time::interval;

const NETWORK_BATCH_SIZE: usize = 100;

pub struct Client {
    id: ClientId,
    network: Network,
    client_data: ClientData,
    config: ClientConfig,
    active_server: NodeId,
    final_request_count: Option<usize>,
    next_request_id: usize,
    is_server_active: bool,
}

impl Client {
    pub async fn new(config: ClientConfig) -> Self {
        let network = Network::new(
            vec![(config.server_id, config.server_address.clone())],
            NETWORK_BATCH_SIZE,
        )
        .await;
        Client {
            id: config.server_id,
            network,
            client_data: ClientData::new(),
            active_server: config.server_id,
            config,
            final_request_count: None,
            next_request_id: 0,
            is_server_active: true,
        }
    }

    pub async fn run(&mut self) {
        // Wait for server to signal start
        info!("{}: Waiting for start signal from server", self.id);
        match self.network.server_messages.recv().await {
            Some(ServerMessage::StartSignal(start_time)) => {
                Self::wait_until_sync_time(&mut self.config, start_time).await;
            }
            _ => panic!("Error waiting for start signal"),
        }

        // Early end
        let intervals = self.config.requests.clone();
        if intervals.is_empty() {
            self.save_results().expect("Failed to save results");
            return;
        }

        // Initialize intervals
        let mut rng = rand::thread_rng();
        let mut intervals = intervals.iter();
        let first_interval = intervals.next().unwrap();
        let mut read_ratio = first_interval.get_read_ratio();
        let mut request_interval = interval(first_interval.get_request_delay());
        let mut next_interval = interval(first_interval.get_interval_duration());
        let _ = next_interval.tick().await;

        let killed_links_req = match self.config.kill_links_requests.clone(){
            Some(links) => links,
            None => Vec::with_capacity(10000),
        };
        let mut killed_links_iter= killed_links_req.iter();
        let mut killed_interval =  interval(Duration::from_secs(999999));
        let mut killed_links = killed_links_iter.next();
        if let Some(links) = killed_links {
            killed_interval = interval(links.get_duration_till_trigger());
            let _ = killed_interval.tick().await;
        }

        let disconnected_node_req = match self.config.disconnect_node_requests.clone(){
            Some(nodes) => nodes,
            None => Vec::with_capacity(10000),
        };
        let mut disconnected_node_iter= disconnected_node_req.iter();
        let mut disconnected_interval =  interval(Duration::from_secs(999999));
        let mut disconnected_node = disconnected_node_iter.next();
        if let Some(node) = disconnected_node{
            disconnected_interval = interval(node.get_duration_till_trigger());
            let _ = disconnected_interval.tick().await;
        }

        let connected_links_req = match self.config.connect_links_requests.clone() {
            Some(links) => links,
            None => Vec::with_capacity(10000),
        };
        let mut connected_links_iter = connected_links_req.iter();
        let mut connect_links_interval = interval(Duration::from_secs(999999));
        let mut connected_links = connected_links_iter.next();
        if let Some(links) = connected_links {
            connect_links_interval = interval(links.get_duration_till_trigger());
            let _ = connect_links_interval.tick().await;
        }

        let connected_node_req = match self.config.connect_node_requests.clone(){
            Some(nodes) => nodes,
            None => Vec::with_capacity(10000),
        };
        let mut connected_node_iter= connected_node_req.iter();
        let mut connect_node_interval =  interval(Duration::from_secs(999999));
        let mut connected_node = connected_node_iter.next();
        if let Some(node) = connected_node{
            connect_node_interval = interval(node.get_duration_till_trigger());
            let _ = connect_node_interval.tick().await;
        }

        // Main event loop
        info!("{}: Starting requests", self.id);
        loop {
            tokio::select! {
                biased;
                Some(msg) = self.network.server_messages.recv() => {
                    self.handle_server_message(msg);
                    if self.run_finished() {
                        break;
                    }
                }
                _ = request_interval.tick(), if self.final_request_count.is_none() => {
                    if self.is_server_active {
                        let is_write = rng.gen::<f64>() > read_ratio;
                        self.send_request(is_write).await;
                    }
                },
                _ = next_interval.tick() => {
                    match intervals.next() {
                        Some(new_interval) => {
                            read_ratio = new_interval.read_ratio;
                            next_interval = interval(new_interval.get_interval_duration());
                            next_interval.tick().await;
                            request_interval = interval(new_interval.get_request_delay());
                        },
                        None => {
                            self.final_request_count = Some(self.client_data.request_count());
                            if self.run_finished() {
                                break;
                            }
                        },
                    }
                },
                _ = killed_interval.tick() => {
                    if let Some(links) = killed_links {
                        self.send_kill_links(links.clone().links).await;
                    }

                    if let Some(next_killed_links) = killed_links_iter.next() {
                        killed_interval = interval(next_killed_links.get_duration_till_trigger());
                        let _ = killed_interval.tick().await;
                        killed_links = Some(next_killed_links);
                    }
                    else{
                        killed_links = None;
                    }
                }
                _ = disconnected_interval.tick() => {
                    if let Some(_) = disconnected_node {
                        self.send_disconnect_node().await;
                    }

                    if let Some(next_disconnected_node) = disconnected_node_iter.next() {
                        disconnected_interval = interval(next_disconnected_node.get_duration_till_trigger());
                        let _ = disconnected_interval.tick().await;
                        disconnected_node = Some(next_disconnected_node);
                    }
                    else{
                        disconnected_node = None;
                    }
                }
                _ = connect_links_interval.tick() => {
                    if let Some(links) = connected_links {
                        self.send_connect_links(links.clone().links).await;
                    }

                    if let Some(next_connected_links) = connected_links_iter.next() {
                        connect_links_interval = interval(next_connected_links.get_duration_till_trigger());
                        let _ = connect_links_interval.tick().await;
                        connected_links = Some(next_connected_links);
                    }
                    else{
                        connected_links = None;
                    }
                }
                _ = connect_node_interval.tick() => {
                    if let Some(_) = connected_node {
                        self.send_connect_node().await;
                    }

                    if let Some(next_connected_node) = connected_node_iter.next() {
                        connect_node_interval = interval(next_connected_node.get_duration_till_trigger());
                        let _ = connect_node_interval.tick().await;
                        connected_node = Some(next_connected_node);
                    }
                    else{
                        connected_node = None;
                    }
                }
            }
        }

        info!(
            "{}: Client finished: collected {} responses",
            self.id,
            self.client_data.response_count(),
        );
        self.network.shutdown();
        self.save_results().expect("Failed to save results");
    }

    fn handle_server_message(&mut self, msg: ServerMessage) {
        debug!("Recieved {msg:?}");
        match msg {
            ServerMessage::StartSignal(_) => (),
            server_response => {
                let cmd_id = server_response.command_id();
                self.client_data.new_response(cmd_id);
            }
        }
    }

    async fn send_request(&mut self, is_write: bool) {
        let key = self.next_request_id.to_string();
        let cmd = match is_write {
            true => KVCommand::Put(key.clone(), key),
            false => KVCommand::Get(key),
        };
        let request = ClientMessage::Append(self.next_request_id, cmd);
        debug!("Sending {request:?}");
        self.network.send(self.active_server, request).await;
        self.client_data.new_request(is_write);
        self.next_request_id += 1;
    }
    async fn send_kill_links(&mut self, links : Vec<NodeId>) {
        for link in links {
            let request = ClientMessage::KillLink(link);
            debug!("Sending {request:?}");
            self.network.send(self.active_server, request).await;
        }
    }
    async fn send_disconnect_node(&mut self) {
        let request = ClientMessage::DisconnectNode();
        debug!("Sending {request:?}");
        self.network.send(self.active_server, request).await;
        self.is_server_active = false;
    }
    async fn send_connect_links(&mut self, links: Vec<NodeId>) {
        for link in links {
            let request = ClientMessage::ConnectLink(link);
            debug!("Sending {request:?}");
            self.network.send(self.active_server, request).await;
        }
    }
    async fn send_connect_node(&mut self) {
        let request = ClientMessage::ConnectNode();
        debug!("Sending {request:?}");
        self.network.send(self.active_server, request).await;
        self.is_server_active = true;
    }
    fn run_finished(&self) -> bool {
        if let Some(count) = self.final_request_count {
            if self.client_data.request_count() >= count {
                return true;
            }
        }
        return false;
    }

    // Wait until the scheduled start time to synchronize client starts.
    // If start time has already passed, start immediately.
    async fn wait_until_sync_time(config: &mut ClientConfig, scheduled_start_utc_ms: i64) {
        // // Desync the clients a bit
        // let mut rng = rand::thread_rng();
        // let scheduled_start_utc_ms = scheduled_start_utc_ms + rng.gen_range(1..100);
        let now = Utc::now();
        let milliseconds_until_sync = scheduled_start_utc_ms - now.timestamp_millis();
        config.sync_time = Some(milliseconds_until_sync);
        if milliseconds_until_sync > 0 {
            tokio::time::sleep(Duration::from_millis(milliseconds_until_sync as u64)).await;
        } else {
            warn!("Started after synchronization point!");
        }
    }

    fn save_results(&self) -> Result<(), std::io::Error> {
        self.client_data.save_summary(self.config.clone())?;
        self.client_data
            .to_csv(self.config.output_filepath.clone())?;
        Ok(())
    }
}
