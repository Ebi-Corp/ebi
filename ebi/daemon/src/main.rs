#![allow(dead_code)]
mod query;
mod services;
mod sharedref;
mod shelf;
mod stateful;
mod tag;
mod workspace;

pub mod prelude {
    pub use crate::sharedref::{ImmutRef, Ref, SharedRef, StatefulRef};
    pub use std::sync::Arc;
    pub use uuid::Uuid;
}

use crate::prelude::*;
use crate::services::peer::{Client, Peer};
use crate::services::prelude::*;
use crate::services::rpc::{DaemonInfo, RequestId, TaskID};
use anyhow::Result;
use ebi_proto::rpc::*;
use iroh::{Endpoint, NodeId, SecretKey};
use paste::paste;
use std::collections::HashMap;
use tokio::sync::{RwLock, mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tower::{Service, ServiceBuilder};
use tracing::{Level, span};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

const HEADER_SIZE: usize = 10; //[!] Move to Constant file 
const ALPN: &[u8] = b"ebi";
const CLIENT_ADDR: &str = "127.0.0.1:3000";

macro_rules! generate_request_match {
    ($msg_code:expr, $buffer:expr, $service:expr, $socket:expr, $($req_ty:ty),* $(,)?) => {
        paste! {
            match $msg_code.try_into() {
                $(
                    Ok(RequestCode::[<$req_ty>]) => {
                        let req = <$req_ty>::decode(&$buffer[..]).unwrap();
                        if let Ok(response) = $service.call(req).await {
                            let mut payload = Vec::new();
                            let _ = ebi_proto::rpc::Message::encode(&response, &mut payload);
                            let mut response_buf = vec![0; HEADER_SIZE];
                            response_buf[0] = MessageType::Response as u8;
                            response_buf[1] = RequestCode::[<$req_ty>]  as u8;
                            let size = payload.len() as u64;
                            tracing::trace!("Response with request code: {:?} and payload size: {}", RequestCode::[<$req_ty>], size);
                            response_buf[2..HEADER_SIZE].copy_from_slice(&size.to_le_bytes());
                            response_buf.extend_from_slice(&payload);
                            let _ = $socket.write_all(&response_buf).await;
                        }
                    }
                )*
                Ok(_) => {
                    todo!();
                }
                Err(_) => {
                    tracing::error!("Unknown response code: {}", $msg_code);
                }
            }
        }
    };
}

macro_rules! generate_response_match {
    ($msg_code:expr, $buffer:expr, $service:expr, $socket:expr, $($req_ty:ty),* $(,)?) => {
        paste! {
            match $msg_code.try_into() {
                $(
                    Ok(RequestCode::[<$req_ty>]) => {
                        let req = <$req_ty>::decode(&$buffer[..]).unwrap();
                        let _ = $service.call(req).await;
                    }
                )*
                Ok(_) => {
                    todo!();
                }
                Err(_) => {
                    tracing::error!("Unknown response code: {}", $msg_code);
                }
            }
        }
    };
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let listener = TcpListener::bind(CLIENT_ADDR).await.unwrap();

    tracing::debug!("Listening for client connections on {}", CLIENT_ADDR);

    let sec_key: [u8; 32] = [
        0, 100, 165, 138, 15, 92, 235, 75, 150, 109, 189, 27, 121, 0, 209, 101, 101, 234, 31, 236,
        60, 34, 88, 28, 130, 189, 26, 107, 97, 32, 177, 252,
    ];
    let ep = Endpoint::builder()
        .discovery_n0()
        .secret_key(SecretKey::from_bytes(&sec_key))
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await;

    let Ok(ep) = ep else {
        tracing::error!("Could not setup iroh node: {}", ep.unwrap_err());
        panic!();
    };

    tracing::info!("Set up iroh endpoint with peer ID {}", ep.node_id());

    let peers = Arc::new(RwLock::new(HashMap::<NodeId, Peer>::new()));
    let clients = Arc::new(RwLock::new(Vec::<Client>::new()));
    let tasks = Arc::new(HashMap::<TaskID, JoinHandle<()>>::new());
    let responses = Arc::new(RwLock::new(HashMap::<RequestId, Response>::new()));
    let id: NodeId = ep.node_id();
    let (broadcast, watcher) = watch::channel::<RequestId>(Uuid::new_v4());

    //[/] The Peer service subscribes to the ResponseHandler when a request is sent.
    //[/] It is then notified when a response is received so it can acquire the read lock on the Response map.
    let daemon_info = Arc::new(DaemonInfo::new(id, "".to_string()));

    let state_srv = StateService::new();

    let peer_srv = PeerService {
        peers: peers.clone(),
        clients: clients.clone(),
        responses: responses.clone(),
    };

    let fs_srv = FileSysService {
        nodes: Arc::new(papaya::HashSet::new()),
    };
    let query_srv = QueryService {
        peer_srv: peer_srv.clone(),
        cache: CacheService {},
        filesys: fs_srv.clone(),
        state_srv: state_srv.clone(),
        daemon_info: daemon_info.clone(),
    };
    let service = ServiceBuilder::new().service(RpcService {
        daemon_info: daemon_info.clone(),
        peer_srv: peer_srv.clone(),
        state_srv: state_srv.clone(),
        fs_srv,
        query_srv,
        responses: responses.clone(),
        tasks: tasks.clone(),
        broadcast: broadcast.clone(),
        watcher: watcher.clone(),
    });
    let mut client_streams = 0;
    //let state = state_srv.state.clone();

    loop {
        tokio::select! {
            Ok((stream, addr)) = listener.accept() => {
                client_streams += 1;
                tracing::info!("Accepted client connection at {}", addr);
                let service = service.clone();
                stream.set_nodelay(true)?;
                let (read, write) = stream.into_split();
                let (tx, mut rx) = mpsc::channel::<(Uuid, Vec<u8>)>(64);
                let client = Client {
                    id,
                    addr,
                    sender: tx.clone(),
                    watcher: watcher.clone()
                };
                clients.write().await.push(client);
                let broadcast = broadcast.clone();

                // Client handling thread
                tokio::spawn(async move {
                    let c_span = span!(Level::INFO, "client", stream_n = client_streams);

                    let mut s_handle = StreamHandler { r_socket: read, w_socket: write, service: service.clone() };
                    loop {
                        let _enter = c_span.enter();
                        tokio::select! {
                            alive = s_handle.listen_ref() => {
                                if !alive {
                                    tracing::info!("client connection closed");
                                    break;
                                }
                            }

                            Some((uuid, bytes)) = rx.recv() => {
                                if s_handle.w_socket.write_all(&bytes).await.is_err() {
                                    let _ = tx.send((uuid, Vec::new())).await; // Empty vec = error
                                                                               // on write ?

                                    let _ = broadcast.send(uuid); // broadcast update
                                }
                            }
                        }
                    }
                });
            },
            conn = ep.accept() => {
                let conn = conn.unwrap().await?;
                //[!] Check if Peer has authorisation to connect (and if permissions match)
                let service = service.clone();
                let (tx, mut rx) = mpsc::channel::<(Uuid, Vec<u8>)>(64);
                // [!] Handle multiple incoming connections per Peer
                let peer = Peer {
                    id: conn.remote_node_id().unwrap(),
                    watcher: watcher.clone(),
                    sender: tx.clone()
                };

                let peer_id = peer.id.to_string();
                peers.write().await.insert(conn.remote_node_id().unwrap(), peer);
                let broadcast = broadcast.clone();

                // Peer handling thread
                tokio::spawn(async move {
                    let mut stream_set = JoinSet::new();

                    let p_span = span!(Level::INFO, "peer handler", id = peer_id.clone());

                    loop {
                        let _enter = p_span.enter();
                        tokio::select! {
                            Ok((write, read)) = conn.accept_bi() => {
                                let s_handle = StreamHandler { r_socket: read, w_socket: write, service: service.clone() };
                                stream_set.spawn(async move { s_handle.listen_owned().await });
                            }
                            Some(res) = stream_set.join_next() => {
                                if let Ok(Some(s_handle)) = res {
                                    stream_set.spawn(async move { s_handle.listen_owned().await });
                                } else {
                                    tracing::info!("Peer {} stream closed", peer_id.clone());
                                }
                            }
                            Some((uuid, bytes)) = rx.recv() => {
                                if let Ok((mut write, read)) = conn.open_bi().await {
                                    if write.write_all(&bytes).await.is_err() {
                                        let _ = tx.send((uuid, Vec::new())).await; // Empty vec = error

                                        let _ = broadcast.send(uuid); // broadcast update
                                    }
                                    let s_handle = StreamHandler { r_socket: read, w_socket: write, service: service.clone() };
                                    stream_set.spawn(async move { s_handle.listen_owned().await });
                                }
                            }
                        }
                    }
                });
            }
        }
    }
}

struct StreamHandler<U, B>
where
    U: AsyncReadExt + Unpin + std::fmt::Debug,
    B: AsyncWriteExt + Unpin + std::fmt::Debug,
{
    r_socket: U,
    w_socket: B,
    service: RpcService,
}

impl<U, B> StreamHandler<U, B>
where
    U: AsyncReadExt + Unpin + std::fmt::Debug,
    B: AsyncWriteExt + Unpin + std::fmt::Debug,
{
    async fn listen_owned(mut self) -> Option<Self> {
        if self.listen_ref().await {
            Some(self)
        } else {
            None
        }
    }

    async fn listen_ref(&mut self) -> bool {
        let mut header = vec![0; HEADER_SIZE];
        let mut service = self.service.clone();

        match self.r_socket.read_exact(&mut header).await {
            Ok(0) => {
                return false;
            }
            Ok(n) => {
                tracing::trace!("Read {} bytes from header", n);
            }
            Err(e) => {
                tracing::error!("Could not read header: {}", e);
                return false;
            }
        };

        let msg_type: u8 = u8::from_le_bytes([header[0]]);
        let msg_code: u8 = u8::from_le_bytes([header[1]]);
        let size: u64 = u64::from_le_bytes(header[2..HEADER_SIZE].try_into().unwrap());

        tracing::trace!(
            "Received message type: {} code: {} size: {}",
            msg_type,
            msg_code,
            size
        );
        let mut buffer = vec![0u8; size as usize];

        match self.r_socket.read_exact(&mut buffer).await {
            Ok(0) => {
                return false;
            }
            Ok(n) => {
                tracing::trace!("Read {} bytes from payload", n);
            }
            Err(e) => {
                tracing::error!("Could not read payload of size {}: {}", size, e);
                return false;
            }
        };
        match msg_type.try_into() {
            Ok(MessageType::Request) => {
                generate_request_match!(
                    msg_code,
                    buffer,
                    service,
                    self.w_socket,
                    CreateTag,
                    ClientQuery,
                    PeerQuery,
                    CreateWorkspace,
                    EditWorkspace,
                    GetWorkspaces,
                    GetShelves,
                    EditTag,
                    DeleteWorkspace,
                    AddShelf,
                    EditShelf,
                    RemoveShelf,
                    AttachTag,
                    DetachTag,
                );
            }
            Ok(MessageType::Response) => {
                generate_response_match!(
                    msg_code,
                    buffer,
                    service,
                    self.w_socket,
                    CreateTag,
                    CreateWorkspace,
                    EditWorkspace,
                    GetWorkspaces,
                    GetShelves,
                    EditTag,
                    DeleteWorkspace,
                    AddShelf,
                    EditShelf,
                    RemoveShelf,
                    AttachTag,
                    DetachTag,
                );
            }
            Ok(MessageType::Data) => {
                // Handle data messages here
            }
            Ok(MessageType::Notification) => {
                // Handle notification messages here
            }
            Ok(MessageType::Sync) => {
                // Handle sync messages here
            }
            Err(_) => {
                tracing::error!("unknown message type: {}", msg_type);
            }
        }
        true
    }
}

fn get_secret_key() -> SecretKey {
    let mut rng = rand::rngs::OsRng;
    iroh_base::SecretKey::generate(&mut rng)
}
