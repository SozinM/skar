use crate::{
    EndpointConfig, Error, GetBlockNumber, GetLogs, LimitConfig, Result, RpcRequest,
    RpcRequestImpl, RpcResponse,
};
use async_std::{
    channel::{bounded as channel, Receiver, Sender},
    sync::{Arc, RwLock},
    task,
};
use skar_format::BlockNumber;
use std::{
    cmp,
    num::{NonZeroU64, NonZeroUsize},
    time::{Duration, Instant},
};
use surf::http::Method;

#[derive(Debug)]
pub struct Endpoint {
    url: Arc<surf::Url>,
    last_block: Arc<RwLock<Option<BlockNumber>>>,
    job_tx: Sender<Job>,
}

impl Endpoint {
    pub fn new(http_client: Arc<surf::Client>, config: EndpointConfig) -> Self {
        let last_block = Arc::new(RwLock::new(None));
        let url = Arc::new(config.url);

        task::spawn(
            WatchHealth {
                http_client: http_client.clone(),
                last_block: last_block.clone(),
                status_refresh_interval_secs: config.status_refresh_interval_secs,
                url: url.clone(),
            }
            .watch(),
        );

        let (job_tx, job_rx) = channel(1);

        task::spawn(
            Listen {
                http_client,
                job_rx,
                limit_config: config.limit,
                window_num_reqs: 0,
                last_limit_refresh: Instant::now(),
                url: url.clone(),
            }
            .listen(),
        );

        Self {
            url,
            last_block,
            job_tx,
        }
    }

    pub fn url(&self) -> &surf::Url {
        &self.url
    }

    pub async fn send(&self, req: Arc<RpcRequest>) -> Result<RpcResponse> {
        if let Some(requirement) = Self::calculate_required_last_block(&req) {
            match *self.last_block.read().await {
                Some(last_block) if requirement <= last_block => (),
                _ => return Err(Error::EndpointTooBehind),
            }
        }

        let (res_tx, res_rx) = channel(1);

        self.job_tx.send(Job { res_tx, req }).await.ok().unwrap();

        res_rx.recv().await.unwrap()
    }

    fn calculate_required_last_block(req: &RpcRequest) -> Option<BlockNumber> {
        match req {
            RpcRequest::Single(req) => Self::calculate_required_last_block_impl(req),
            RpcRequest::Batch(reqs) => reqs.iter().fold(None, |acc, req| {
                match (acc, Self::calculate_required_last_block_impl(req)) {
                    (Some(a), Some(b)) => Some(cmp::max(a, b)),
                    (None, v) => v,
                    (v, None) => v,
                }
            }),
        }
    }

    fn calculate_required_last_block_impl(req: &RpcRequestImpl) -> Option<BlockNumber> {
        match req {
            RpcRequestImpl::GetLogs(GetLogs { to_block, .. }) => Some(*to_block),
            RpcRequestImpl::GetBlockNumber => None,
            RpcRequestImpl::GetBlockByNumber(block_number) => Some(*block_number),
            RpcRequestImpl::GetTransactionReceipt(block_number, _) => Some(*block_number),
        }
    }
}

struct WatchHealth {
    url: Arc<surf::Url>,
    http_client: Arc<surf::Client>,
    last_block: Arc<RwLock<Option<BlockNumber>>>,
    status_refresh_interval_secs: NonZeroU64,
}

impl WatchHealth {
    async fn watch(self) {
        loop {
            let (res_tx, res_rx) = channel(1);

            let req = Arc::new(GetBlockNumber.into());

            task::spawn(
                SendRpcRequest {
                    url: self.url.clone(),
                    http_client: self.http_client.clone(),
                    job: Job { res_tx, req },
                }
                .send(),
            );

            match res_rx.recv().await.unwrap() {
                Ok(resp) => {
                    *self.last_block.write().await = Some(resp.try_into_single().unwrap());
                }
                Err(e) => {
                    *self.last_block.write().await = None;
                    log::error!(
                        "Failed to get last block for {}. Caused By:\n{}",
                        self.url,
                        e
                    );
                }
            }

            task::sleep(Duration::from_secs(self.status_refresh_interval_secs.get())).await;
        }
    }
}

struct Job {
    req: Arc<RpcRequest>,
    res_tx: Sender<Result<RpcResponse>>,
}

struct Listen {
    url: Arc<surf::Url>,
    http_client: Arc<surf::Client>,
    job_rx: Receiver<Job>,
    limit_config: LimitConfig,
    window_num_reqs: usize,
    last_limit_refresh: Instant,
}

impl Listen {
    async fn listen(mut self) {
        while let Ok(job) = self.job_rx.recv().await {
            if let Err(e) = self.update_limit(&job.req) {
                task::spawn(async move {
                    job.res_tx.send(Err(e)).await.ok();
                });
                continue;
            }

            task::spawn(
                SendRpcRequest {
                    http_client: self.http_client.clone(),
                    job,
                    url: self.url.clone(),
                }
                .send(),
            );
        }
    }

    fn update_limit(&mut self, req: &RpcRequest) -> Result<()> {
        let needed_reqs = self.calculate_needed_reqs(req);

        if self.last_limit_refresh.elapsed().as_millis()
            >= self.limit_config.req_limit_window_ms.get()
        {
            self.last_limit_refresh = Instant::now();
            self.window_num_reqs = 0;
        }

        if self.window_num_reqs + needed_reqs.get() < self.limit_config.req_limit.get() {
            self.window_num_reqs += needed_reqs.get();
            Ok(())
        } else {
            Err(Error::EndpointLimitTooLow)
        }
    }

    fn calculate_needed_reqs(&self, req: &RpcRequest) -> NonZeroUsize {
        match req {
            RpcRequest::Single(req) => self.calculate_needed_reqs_impl(req),
            RpcRequest::Batch(reqs) => {
                let needed_reqs_for_batch = |batch: &[RpcRequestImpl]| {
                    // start folding from 1 and add any extra required requests
                    batch.iter().fold(1, |acc, req| {
                        acc + self.calculate_needed_reqs_impl(req).get() - 1
                    })
                };

                let needed_reqs = reqs
                    .chunks(self.limit_config.batch_size_limit.get())
                    .map(needed_reqs_for_batch)
                    .sum();

                NonZeroUsize::new(needed_reqs).unwrap()
            }
        }
    }

    fn calculate_needed_reqs_impl(&self, req: &RpcRequestImpl) -> NonZeroUsize {
        match req {
            RpcRequestImpl::GetLogs(GetLogs {
                from_block,
                to_block,
            }) => {
                let range_limit = self.limit_config.get_logs_range_limit.get();
                let range = *to_block - *from_block + 1.into();
                let range: u64 = range.into();
                let num_reqs = (range + range_limit - 1) / range_limit;

                NonZeroUsize::new(num_reqs.try_into().unwrap()).unwrap()
            }
            _ => NonZeroUsize::new(1).unwrap(),
        }
    }
}

struct SendRpcRequest {
    url: Arc<surf::Url>,
    http_client: Arc<surf::Client>,
    job: Job,
}

impl SendRpcRequest {
    async fn send(self) -> Result<RpcResponse> {
        let json: serde_json::Value = self.job.req.as_ref().into();

        let req = surf::Request::builder(Method::Post, surf::Url::clone(&self.url))
            .body_json(&json)
            .unwrap();

        let res = self
            .http_client
            .recv_string(req)
            .await
            .map_err(Error::HttpRequest)?;

        self.job
            .req
            .resp_from_json(&res)
            .ok_or_else(|| Error::InvalidRPCResponse(res))
    }
}

#[cfg(test)]
mod tests {
    use crate::{GetLogs, RpcRequest, RpcRequestImpl};
    use hex_literal::hex;

    use super::*;

    #[test]
    fn test_calculate_required_last_block() {
        let req = RpcRequest::Batch(vec![
            RpcRequestImpl::GetBlockNumber,
            RpcRequestImpl::GetLogs(GetLogs {
                from_block: 15.into(),
                to_block: 17.into(),
            }),
            RpcRequestImpl::GetBlockByNumber(199.into()),
            RpcRequestImpl::GetBlockNumber,
        ]);

        let req = Endpoint::calculate_required_last_block(&req);

        assert_eq!(req, Some(199.into()));
    }

    #[test]
    fn test_calculate_required_last_block_get_logs() {
        let req = Endpoint::calculate_required_last_block_impl(&RpcRequestImpl::GetLogs(GetLogs {
            from_block: 15.into(),
            to_block: 30.into(),
        }));

        assert_eq!(req, Some(30.into()));
    }

    #[test]
    fn test_calculate_required_last_block_get_block_number() {
        let req = Endpoint::calculate_required_last_block_impl(&RpcRequestImpl::GetBlockNumber);

        assert!(req.is_none());
    }

    #[test]
    fn test_calculate_required_last_block_get_block_by_number() {
        let req = Endpoint::calculate_required_last_block_impl(&RpcRequestImpl::GetBlockByNumber(
            69.into(),
        ));

        assert_eq!(req, Some(69.into()));
    }

    #[test]
    fn test_calculate_required_last_block_get_transaction_receipt() {
        let req =
            Endpoint::calculate_required_last_block_impl(&RpcRequestImpl::GetTransactionReceipt(
                691.into(),
                hex!("017e8ad62f871604544a2ac9ea80ce920a0c79c30f11440a7b481ece7f18b2b0").into(),
            ));

        assert_eq!(req, Some(691.into()));
    }

    #[test]
    fn test_update_limit() {
        let (_job_tx, job_rx) = channel(1);
        let mut listen = Listen {
            url: surf::Url::parse("http://hello.com").unwrap().into(),
            http_client: surf::Client::default().into(),
            job_rx,
            limit_config: LimitConfig {
                req_limit: 50.try_into().unwrap(),
                req_limit_window_ms: 1000.try_into().unwrap(),
                get_logs_range_limit: 5.try_into().unwrap(),
                batch_size_limit: 5.try_into().unwrap(),
            },
            window_num_reqs: 0,
            last_limit_refresh: Instant::now(),
        };

        let res = listen.update_limit(&RpcRequest::Batch(
            std::iter::repeat(RpcRequestImpl::GetBlockNumber)
                .take(300)
                .collect(),
        ));

        assert!(res.is_err());
        assert_eq!(listen.window_num_reqs, 0);

        let res = listen.update_limit(&RpcRequest::Batch(
            std::iter::repeat(RpcRequestImpl::GetLogs(GetLogs {
                from_block: 1.into(),
                to_block: 7.into(),
            }))
            .take(2)
            .collect(),
        ));

        assert!(res.is_ok());
        assert_eq!(listen.window_num_reqs, 3);
    }

    #[test]
    fn test_calculate_needed_reqs() {
        let (_job_tx, job_rx) = channel(1);
        let listen = Listen {
            url: surf::Url::parse("http://hello.com").unwrap().into(),
            http_client: surf::Client::default().into(),
            job_rx,
            limit_config: LimitConfig {
                req_limit: 50.try_into().unwrap(),
                req_limit_window_ms: 1000.try_into().unwrap(),
                get_logs_range_limit: 5.try_into().unwrap(),
                batch_size_limit: 5.try_into().unwrap(),
            },
            window_num_reqs: 0,
            last_limit_refresh: Instant::now(),
        };

        let n = listen.calculate_needed_reqs(&RpcRequest::Single(RpcRequestImpl::GetBlockNumber));
        assert_eq!(n.get(), 1);

        let n = listen.calculate_needed_reqs(&RpcRequest::Batch(
            std::iter::repeat(RpcRequestImpl::GetBlockNumber)
                .take(301)
                .collect(),
        ));
        assert_eq!(n.get(), 61);

        let n = listen.calculate_needed_reqs(
            &GetLogs {
                from_block: 1.into(),
                to_block: 16.into(),
            }
            .into(),
        );
        assert_eq!(n.get(), 4);
    }
}
