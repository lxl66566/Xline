use std::{collections::HashMap, sync::Arc, time::Duration};

use itertools::Itertools;
use jepsen_rs::client::ElleRwClusterClient;
use madsim::runtime::NodeHandle;
use tonic::transport::Channel;
use tracing::debug;
use utils::config::{
    AuthConfig, ClientConfig, ClusterConfig, CompactConfig, CurpConfig, InitialClusterState,
    ServerTimeout, StorageConfig, TlsConfig,
};
use xline::server::XlineServer;
use xline_client::{
    error::XlineClientError,
    types::{
        kv::{CompactionResponse, PutOptions, PutResponse, RangeOptions, RangeResponse},
        watch::{WatchOptions, WatchStreaming, Watcher},
    },
    Client, ClientOptions,
};
use xlineapi::{
    command::Command, ClusterClient, KvClient, MemberAddResponse, MemberListResponse, RequestUnion,
    WatchClient,
};

pub struct XlineNode {
    pub client_url: String,
    pub peer_url: String,
    pub name: String,
    pub handle: NodeHandle,
}

pub struct XlineGroup {
    pub nodes: HashMap<String, XlineNode>,
    pub client_handle: NodeHandle,
}

impl XlineGroup {
    pub async fn new(size: usize) -> Self {
        assert!(size >= 3, "the number of nodes must >= 3");
        let handle = madsim::runtime::Handle::current();

        let all: HashMap<_, _> = (0..size)
            .map(|x| (format!("S{x}"), vec![format!("192.168.1.{}:2380", x + 1)]))
            .collect();
        let nodes = (0..size)
            .map(|i| {
                let name = format!("S{i}");
                let client_url = format!("192.168.1.{}:2379", i + 1);
                let peer_url = format!("192.168.1.{}:2380", i + 1);
                let cluster_config = ClusterConfig::new(
                    name.clone(),
                    vec!["0.0.0.0:2380".to_owned()],
                    vec![format!("192.168.1.{}:2380", i + 1)],
                    vec!["0.0.0.0:2379".to_owned()],
                    vec![format!("192.168.1.{}:2379", i + 1)],
                    all.clone(),
                    i == 0,
                    CurpConfig::default(),
                    ClientConfig::default(),
                    ServerTimeout::default(),
                    InitialClusterState::New,
                );

                let handle = handle
                    .create_node()
                    .name(name.clone())
                    .ip(format!("192.168.1.{}", i + 1).parse().unwrap())
                    .init(move || {
                        let cluster_config = cluster_config.clone();
                        async move {
                            let server = XlineServer::new(
                                cluster_config,
                                StorageConfig::default(),
                                CompactConfig::default(),
                                AuthConfig::default(),
                                TlsConfig::default(),
                            )
                            .await
                            .unwrap();
                            server
                                .start_from_single_addr(
                                    "0.0.0.0:2379".parse().unwrap(),
                                    "0.0.0.0:2380".parse().unwrap(),
                                )
                                .await
                                .unwrap()
                                .await
                                .unwrap()
                                .unwrap();
                        }
                    })
                    .build();
                (
                    name.clone(),
                    XlineNode {
                        client_url,
                        peer_url,
                        name,
                        handle,
                    },
                )
            })
            .collect();
        let client_handle = handle
            .create_node()
            .name("client")
            .ip("192.168.2.1".parse().unwrap())
            .build();
        madsim::time::sleep(Duration::from_secs(20)).await;
        Self {
            nodes,
            client_handle,
        }
    }

    pub async fn client(&self) -> SimClient {
        let all_members = self
            .nodes
            .values()
            .map(|node| node.client_url.clone())
            .collect_vec();
        let client = self
            .client_handle
            .spawn(async move {
                Client::connect(all_members, ClientOptions::default())
                    .await
                    .unwrap()
            })
            .await
            .unwrap();
        SimClient {
            inner: Arc::new(client),
            handle: self.client_handle.clone(),
        }
    }

    pub fn get_node(&self, name: &str) -> &XlineNode {
        self.nodes.get(name).unwrap()
    }

    pub async fn crash(&mut self, name: &str) {
        let handle = madsim::runtime::Handle::current();
        handle.kill(name);
        madsim::time::sleep(Duration::from_secs(10)).await;
        if !handle.is_exit(name) {
            panic!("failed to crash node: {name}");
        }
    }

    pub async fn restart(&mut self, name: &str) {
        let handle = madsim::runtime::Handle::current();
        handle.restart(name);
    }
}

pub struct SimClient {
    inner: Arc<Client>,
    handle: NodeHandle,
}

impl SimClient {
    pub async fn put(
        &self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        option: Option<PutOptions>,
    ) -> Result<PutResponse, XlineClientError<Command>> {
        let client = self.inner.clone();
        let key = key.into();
        let value = value.into();
        self.handle
            .spawn(async move { client.kv_client().put(key, value, option).await })
            .await
            .unwrap()
    }

    pub async fn range(
        &self,
        key: impl Into<Vec<u8>>,
        options: Option<RangeOptions>,
    ) -> Result<RangeResponse, XlineClientError<Command>> {
        let client = self.inner.clone();
        let key = key.into();
        self.handle
            .spawn(async move { client.kv_client().range(key, options).await })
            .await
            .unwrap()
    }

    pub async fn compact(
        &self,
        revision: i64,
        physical: bool,
    ) -> Result<CompactionResponse, XlineClientError<Command>> {
        let client = self.inner.clone();
        self.handle
            .spawn(async move { client.kv_client().compact(revision, physical).await })
            .await
            .unwrap()
    }

    pub async fn watch(
        &self,
        key: impl Into<Vec<u8>>,
        options: Option<WatchOptions>,
    ) -> Result<(Watcher, WatchStreaming), XlineClientError<Command>> {
        let client = self.inner.clone();
        let key = key.into();
        self.handle
            .spawn(async move { client.watch_client().watch(key, options).await })
            .await
            .unwrap()
    }
}

#[async_trait::async_trait]
impl ElleRwClusterClient for SimClient {
    async fn get(&self, key: u64) -> Result<Option<u64>, String> {
        Ok(self
            .range(key.to_be_bytes(), None)
            .await
            .map_err(|err| err.to_string())?
            .kvs
            .into_iter()
            .next()
            .map(|kv: xlineapi::KeyValue| {
                u64::from_be_bytes(kv.value.try_into().expect("key should be 8 bytes"))
            }))
    }
    async fn put(&self, key: u64, value: u64) -> Result<(), String> {
        self.put(key.to_be_bytes(), value.to_be_bytes(), None)
            .await
            .map_err(|err| err.to_string())?;
        Ok(())
    }
}

impl Drop for XlineGroup {
    fn drop(&mut self) {
        let handle = madsim::runtime::Handle::current();
        for node in self.nodes.values() {
            handle.send_ctrl_c(node.handle.id());
        }
        handle.send_ctrl_c(self.client_handle.id());
        for (name, node) in &self.nodes {
            if !handle.is_exit(node.handle.id()) {
                panic!("failed to graceful shutdown {name}");
            }
        }
        debug!("all nodes shutdowned");
    }
}

pub struct SimEtcdClient {
    watch: WatchClient<Channel>,
    kv: KvClient<Channel>,
    cluster: ClusterClient<Channel>,
    handle: NodeHandle,
}

impl SimEtcdClient {
    pub async fn new(addr: String, handle: NodeHandle) -> Self {
        let (watch, kv, cluster) = handle
            .spawn(async move {
                (
                    WatchClient::connect(addr.clone()).await.unwrap(),
                    KvClient::connect(addr.clone()).await.unwrap(),
                    ClusterClient::connect(addr).await.unwrap(),
                )
            })
            .await
            .unwrap();
        Self {
            watch,
            kv,
            cluster,
            handle,
        }
    }

    pub async fn put(
        &self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        option: Option<PutOptions>,
    ) -> Result<PutResponse, XlineClientError<Command>> {
        let mut client = self.kv.clone();
        let key = key.into();
        let value = value.into();
        self.handle
            .spawn(async move {
                client
                    .put(xlineapi::PutRequest::from(
                        option.unwrap_or_default().with_kv(key, value),
                    ))
                    .await
                    .map(|r| r.into_inner())
                    .map_err(Into::into)
            })
            .await
            .unwrap()
    }

    pub async fn compact(
        &self,
        revision: i64,
        physical: bool,
    ) -> Result<CompactionResponse, XlineClientError<Command>> {
        let mut client = self.kv.clone();
        self.handle
            .spawn(async move {
                client
                    .compact(xlineapi::CompactionRequest { revision, physical })
                    .await
                    .map(|r| r.into_inner())
                    .map_err(Into::into)
            })
            .await
            .unwrap()
    }

    pub async fn watch(
        &self,
        key: impl Into<Vec<u8>>,
        options: Option<WatchOptions>,
    ) -> Result<(Watcher, WatchStreaming), XlineClientError<Command>> {
        let mut client = self.watch.clone();
        let key = key.into();
        self.handle
            .spawn(async move {
                let (mut request_sender, request_receiver) =
                    futures::channel::mpsc::channel::<xlineapi::WatchRequest>(128);

                let request = xlineapi::WatchRequest {
                    request_union: Some(RequestUnion::CreateRequest(
                        options.unwrap_or_default().with_key(key).into(),
                    )),
                };

                request_sender
                    .try_send(request)
                    .map_err(|e| XlineClientError::WatchError(e.to_string()))?;

                let mut response_stream = client.watch(request_receiver).await?.into_inner();

                let watch_id = match response_stream.message().await? {
                    Some(resp) => {
                        assert!(resp.created, "not a create watch response");
                        resp.watch_id
                    }
                    None => {
                        return Err(XlineClientError::WatchError(String::from(
                            "failed to create watch",
                        )));
                    }
                };

                Ok((
                    Watcher::new(watch_id, request_sender.clone()),
                    WatchStreaming::new(response_stream, request_sender),
                ))
            })
            .await
            .unwrap()
    }

    pub async fn member_add<I: Into<String>>(
        &mut self,
        peer_urls: impl Into<Vec<I>>,
        is_learner: bool,
    ) -> Result<MemberAddResponse, XlineClientError<Command>> {
        let mut client = self.cluster.clone();
        let peer_urls: Vec<String> = peer_urls.into().into_iter().map(Into::into).collect();
        self.handle
            .spawn(async move {
                client
                    .member_add(xlineapi::MemberAddRequest {
                        peer_ur_ls: peer_urls,
                        is_learner,
                    })
                    .await
                    .map(|r| r.into_inner())
                    .map_err(Into::into)
            })
            .await
            .unwrap()
    }

    pub async fn member_list(
        &mut self,
        linearizable: bool,
    ) -> Result<MemberListResponse, XlineClientError<Command>> {
        let mut client = self.cluster.clone();
        self.handle
            .spawn(async move {
                client
                    .member_list(xlineapi::MemberListRequest { linearizable })
                    .await
                    .map(|r| r.into_inner())
                    .map_err(Into::into)
            })
            .await
            .unwrap()
    }
}
