use crate::prelude::*;
use ebi_proto::rpc::*;
use iroh::NodeId;
use papaya::HashMap;
use papaya::HashSet;
use std::hash::Hash;
use std::net::SocketAddr;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::{mpsc::Sender, watch::Receiver};
use tokio::time::{Duration, sleep};
use tower::Service;

use crate::services::rpc::RequestId;

const HEADER_SIZE: usize = 10; //[!] Move to Constant file 

#[derive(Debug)]
pub enum PeerError {
    PeerNotFound,
    TimedOut,
    UnexpectedResponse,
    ConnectionClosed, // [TODO] Investigate how to detect connection closed
    Unknown,
}

#[derive(Clone, Debug)]
pub struct PeerService {
    pub peers: Arc<HashMap<NodeId, Peer>>,
    pub clients: Arc<HashSet<Client>>,
    pub responses: Arc<HashMap<RequestId, Response>>,
}

#[derive(Clone, Debug)]
pub struct Peer {
    pub id: NodeId,
    pub watcher: Receiver<Uuid>,
    pub sender: Sender<(Uuid, Vec<u8>)>,
}

#[derive(Clone, Debug)]
pub struct Client {
    pub id: NodeId,
    pub addr: SocketAddr,
    pub sender: Sender<(Uuid, Vec<u8>)>,
    pub watcher: Receiver<Uuid>,
}
impl Hash for Client {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}
impl PartialEq for Client {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for Client {}


async fn wait_call(mut watcher: Receiver<Uuid>, request_uuid: Uuid) {
    let mut id = Uuid::new_v4();
    while id != request_uuid {
        // this returns an error only if sender in main is dropped
        watcher.changed().await.unwrap();
        id = *watcher.borrow_and_update();
    }
}

impl Service<(NodeId, Data)> for PeerService {
    type Response = Uuid;
    type Error = PeerError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: (NodeId, Data)) -> Self::Future {
        let peers = self.peers.clone();
        let clients = self.clients.clone();
        Box::pin(async move {
            let clients = clients.pin_owned();
            let sender = {
                let client = clients.iter().find(|c| c.id == req.0);
                if let Some(client) = client {
                    client.sender.clone()
                } else {
                    peers
                        .pin()
                        .get(&req.0)
                        .ok_or(PeerError::PeerNotFound)?
                        .sender
                        .clone()
                }
            };

            let mut payload = Vec::new();
            let request_uuid = Uuid::new_v4();

            let req = req.1.clone();
            // [TODO] metadata of requests should be checked for in a validation service
            req.metadata().as_mut().unwrap().request_uuid = request_uuid.as_bytes().to_vec();
            req.encode(&mut payload).unwrap();
            let mut buffer = vec![0; HEADER_SIZE];
            buffer[0] = MessageType::Data as u8;
            buffer[1] = req.request_code() as u8;
            let size = payload.len() as u64;
            buffer[2..HEADER_SIZE].copy_from_slice(&size.to_le_bytes());
            buffer.extend_from_slice(&payload);

            sender
                .send((request_uuid, buffer))
                .await
                .map_err(|_| PeerError::ConnectionClosed)?;
            Ok(request_uuid)
        })
    }
}

impl Service<(NodeId, Request)> for PeerService {
    type Response = Response;
    type Error = PeerError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: (NodeId, Request)) -> Self::Future {
        let peers = self.peers.clone();
        let responses = self.responses.clone();
        Box::pin(async move {
            let peers = peers.pin_owned();
            let peer = peers.get(&req.0).ok_or(PeerError::PeerNotFound)?;
            let sender = peer.sender.clone();
            let watcher = peer.watcher.clone();

            let mut payload = Vec::new();
            let request_uuid = Uuid::new_v4();
            let req = req.1.clone();
            // [TODO] metadata of requests should be checked for in a validation service
            req.metadata().as_mut().unwrap().request_uuid = request_uuid.as_bytes().to_vec();
            req.encode(&mut payload).unwrap();
            let mut buffer = vec![0; HEADER_SIZE];
            buffer[0] = MessageType::Request as u8;
            buffer[1] = req.request_code() as u8;
            let size = payload.len() as u64;
            buffer[2..HEADER_SIZE].copy_from_slice(&size.to_le_bytes());
            buffer.extend_from_slice(&payload);

            sender
                .send((request_uuid, buffer))
                .await
                .map_err(|_| PeerError::ConnectionClosed)?;

            tokio::select! {
                _ = sleep(Duration::from_secs(120)) => {
                    Err(PeerError::TimedOut)
                }
                _ = wait_call(watcher, request_uuid) => {
                    if let Some(res) = responses.pin().get(&request_uuid) {
                        let res = res.clone();
                        res.try_into().map_err(|_| PeerError::UnexpectedResponse)
                    } else {
                        Err(PeerError::Unknown)
                    }
                }
            }
        })
    }
}
