#![allow(dead_code)]
use crate::services::peer::{Client, Peer, PeerService};
use crate::services::rpc::{DaemonInfo, RequestId, RpcService, TaskID};
use crate::services::workspace::WorkspaceService;
use anyhow::Result;
use ebi_proto::rpc::*;
use iroh::{Endpoint, NodeId, SecretKey};
use paste::paste;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tower::{Service, ServiceBuilder};
use uuid::Uuid;

mod query;
mod services;
mod shelf;
mod tag;
mod workspace;

const HEADER_SIZE: usize = 10; //[!] Move to Constant file 
const ALPN: &[u8] = b"ebi";

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
                    println!("Unknown response code: {}", $msg_code);
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
                    println!("Unknown response code: {}", $msg_code);
                }
            }
        }
    };
}

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:3000").await.unwrap();
    let ep = loop {
        // Generates a SK/PK pair until an unused one is found
        let sec_key = get_secret_key();
        let ep = Endpoint::builder()
            .discovery_n0()
            .secret_key(sec_key)
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await;
        if let Ok(ep) = ep {
            break ep;
        }
    };
    println!("{:?}", ep.node_addr().await?.node_id.as_bytes());
    println!("{:?}", ep.node_addr().await?.node_id);
    let peers = Arc::new(RwLock::new(HashMap::<NodeId, Peer>::new()));
    let clients = Arc::new(RwLock::new(Vec::<Client>::new()));
    let tasks = Arc::new(HashMap::<TaskID, JoinHandle<()>>::new());
    let responses = Arc::new(RwLock::new(HashMap::<RequestId, Response>::new()));
    let notify_queue = Arc::new(RwLock::new(VecDeque::new()));
    let id: NodeId = ep.node_id();
    let (broadcast, watcher) = watch::channel::<RequestId>(Uuid::now_v7());
    //[/] The Peer service subscribes to the ResponseHandler when a request is sent.
    //[/] It is then notified when a response is received so it can acquire the read lock on the Response map.

    let service = ServiceBuilder::new().service(RpcService {
        daemon_info: Arc::new(DaemonInfo::new(id, "".to_string())),
        peer_srv: PeerService {
            peers: peers.clone(),
            clients: clients.clone(),
            responses: responses.clone(),
        },
        responses: responses.clone(),
        notify_queue: notify_queue.clone(),
        tasks: tasks.clone(),
        workspace_srv: WorkspaceService {
            workspaces: Arc::new(RwLock::new(HashMap::new())),
            shelf_assignment: Arc::new(RwLock::new(HashMap::new())),
            paths: Arc::new(RwLock::new(HashMap::new())),
        },
        broadcast: broadcast.clone(),
        watcher: watcher.clone(),
    });
    loop {
        tokio::select! {
            Ok((stream, addr)) = listener.accept() => {
                let mut service = service.clone();
                let (mut read, mut write) = stream.into_split();
                let (tx, mut rx) = mpsc::channel::<(Uuid, Vec<u8>)>(64);
                let client = Client {
                    hash: 0,
                    addr,
                    send: tx.clone(),
                    watcher: watcher.clone()
                };
                clients.write().await.push(client);
                let broadcast = broadcast.clone();

                // Client handling thread
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            _ = handle_client(&mut read, &mut write, &mut service) => {}
                            Some((uuid, bytes)) = rx.recv() => {
                                if write.write_all(&bytes).await.is_err() {
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
                let mut service = service.clone();
                let (tx, mut rx) = mpsc::channel::<(Uuid, Vec<u8>)>(64);
                // [!] Handle multiple incoming connections per Peer
                let peer = Peer {
                    id: conn.remote_node_id().unwrap(),
                    watcher: watcher.clone(),
                    sender: tx.clone()
                };
                peers.write().await.insert(conn.remote_node_id().unwrap(), peer);
                let broadcast = broadcast.clone();

                // Peer handling thread
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            Ok((mut write, mut read)) = conn.accept_bi() => {
                                handle_client(&mut read, &mut write, &mut service).await;
                            }
                            Some((uuid, bytes)) = rx.recv() => {
                                if let Ok((mut write, mut read)) = conn.open_bi().await {
                                    if write.write_all(&bytes).await.is_err() {
                                        let _ = tx.send((uuid, Vec::new())).await; // Empty vec = error
                                                                               // on write ?

                                        let _ = broadcast.send(uuid); // broadcast update
                                    } else {
                                        handle_client(&mut read, &mut write, &mut service).await;
                                    }
                                }
                            }
                        }
                    }
                });
            }
        }
    }
}

async fn handle_client<U: AsyncReadExt + Unpin, B: AsyncWriteExt + Unpin>(
    r_socket: &mut U,
    w_socket: &mut B,
    service: &mut RpcService,
) {
    let mut header = vec![0; HEADER_SIZE];

    loop {
        let _bytes_read = match r_socket.read_exact(&mut header).await {
            Ok(n) if n == 0 => {
                println!("{}", n);
                break;
            }
            Ok(n) => n,
            Err(e) => {
                println!("{:?}", e);
                9
            }
        };
        let msg_type: u8 = u8::from_le_bytes([header[0]]);
        let msg_code: u8 = u8::from_le_bytes([header[1]]);
        println!("Message Type: {}", msg_type);
        println!("Message Code: {}", msg_code);
        let size: u64 = u64::from_le_bytes(header[2..HEADER_SIZE].try_into().unwrap());
        println!("size: {}", size);
        let mut buffer = vec![0u8; size as usize];
        let _bytes_read = match r_socket.read_exact(&mut buffer).await {
            Ok(n) if n == 0 => 0,
            Ok(n) => n,
            Err(e) => {
                println!("{:?}", e);
                0
            }
        };
        println!("read all");

        match msg_type.try_into() {
            Ok(MessageType::Request) => {
                println!("Request message type received");
                generate_request_match!(
                    msg_code,
                    buffer,
                    service,
                    w_socket,
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
            Ok(MessageType::Response) => {
                println!("Response message type received");
                generate_response_match!(
                    msg_code,
                    buffer,
                    service,
                    w_socket,
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
                println!("Data message type received");
                // Handle data messages here
            }
            Ok(MessageType::Notification) => {
                println!("Notification message type received");
                // Handle notification messages here
            }
            Ok(MessageType::Sync) => {
                println!("Sync message type received");
                // Handle sync messages here
            }
            Err(_) => {
                println!("Unknown message type: {}", msg_type);
                break;
            }
        }
    }
    //println!("Client disconnected: {}", addr);
}

fn get_secret_key() -> SecretKey {
    let mut rng = rand::rngs::OsRng;
    iroh_base::SecretKey::generate(&mut rng)
}
