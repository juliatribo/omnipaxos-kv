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
            None => vec![]
        };
        let mut killed_links_iter= killed_links_req.iter();
        let mut killed_interval =  interval(Duration::from_secs(999999));
        let mut killed_links = killed_links_iter.next();
        if let Some(links) = killed_links {
            killed_interval = interval(links.get_duration_till_trigger());
            let _ = killed_interval.tick().await;
        }

        let disconnected_nodes_req = match self.config.disconnect_nodes_requests.clone(){
            Some(nodes) => nodes,
            None => vec![]
        };
        let mut disconnected_nodes_iter= disconnected_nodes_req.iter();
        let mut disconnected_interval =  interval(Duration::from_secs(999999));
        let mut disconnected_nodes = disconnected_nodes_iter.next();
        if let Some(nodes) = disconnected_nodes{
            disconnected_interval = interval(nodes.get_duration_till_trigger());
            let _ = disconnected_interval.tick().await;
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
                    let is_write = rng.gen::<f64>() > read_ratio;
                    self.send_request(is_write).await;
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
                    if let Some(nodes) = disconnected_nodes {
                        self.send_disconnect_nodes(nodes.clone().nodes).await;
                    }
        
                    if let Some(next_disconnected_nodes) = disconnected_nodes_iter.next() {
                        disconnected_interval = interval(next_disconnected_nodes.get_duration_till_trigger());
                        let _ = disconnected_interval.tick().await;
                        disconnected_nodes = Some(next_disconnected_nodes);
                    }
                    else{
                        disconnected_nodes = None;
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
    async fn send_kill_links(&mut self, links : Vec<(NodeId, NodeId)>) {
        for link in links {
            let request = ClientMessage::KillLink(link.0, link.1);
            debug!("Sending {request:?}");
            self.network.send(self.active_server, request).await;
        }
    }
    async fn send_disconnect_nodes(&mut self, nodes : Vec<NodeId>) {
        for node in nodes {
            let request = ClientMessage::DisconnectNode(node);
            debug!("Sending {request:?}");
            self.network.send(self.active_server, request).await;
        }
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
