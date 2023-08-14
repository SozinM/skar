use crate::config::InnerConfig;
use crate::{validate_batch_data, BatchData, IngestConfig};
use anyhow::{Context, Error, Result};
use futures::{stream, StreamExt};
use skar_format::{Block, BlockNumber, Transaction, TransactionReceipt};
use skar_rpc_client::{GetBlockByNumber, GetBlockNumber, GetBlockReceipts, RpcClient, RpcRequest};
use std::cmp;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

pub struct Ingest {
    data_rx: mpsc::Receiver<BatchData>,
}

impl Ingest {
    pub fn spawn(config: IngestConfig) -> Self {
        let (data_tx, data_rx) = mpsc::channel(4);

        let client = RpcClient::new(config.rpc_client).into();

        tokio::spawn(async move {
            let e = Ingester {
                client,
                data_tx,
                config: config.inner,
            }
            .ingest()
            .await;

            if let Err(e) = e {
                log::error!("failed to run ingester: {:?}", e);
            }
        });

        Self { data_rx }
    }

    pub async fn recv(&mut self) -> Result<BatchData> {
        self.data_rx.recv().await.context("receive batch data")
    }
}

struct Ingester {
    client: Arc<RpcClient>,
    data_tx: mpsc::Sender<BatchData>,
    config: InnerConfig,
}

impl Ingester {
    async fn ingest(self) -> Result<()> {
        let mut next_block = self.initial_sync().await.context("run initial sync")?;
        let mut tip_block_num = 0;

        log::info!("starting to wait for new blocks");

        loop {
            if tip_block_num >= next_block {
                let block = self.client.send(GetBlockByNumber(next_block.into()).into());

                let receipts = self.client.send(GetBlockReceipts(next_block.into()).into());

                let (block, receipts) = futures::join!(block, receipts);

                let block = block.context("get block data")?.try_into_single().unwrap();
                let receipts = receipts
                    .context("get block receipts")?
                    .try_into_single()
                    .unwrap();

                log::trace!("downloaded data for block {}", next_block);

                let data = BatchData {
                    blocks: vec![block],
                    receipts: vec![receipts],
                    from_block: next_block,
                    to_block: next_block + 1,
                };

                validate_batch_data(&data).context("validate batch data")?;

                if self.data_tx.send(data).await.is_err() {
                    log::warn!("quitting ingest loop because the receiver is dropped");
                    break;
                }

                next_block += 1;
            } else {
                tokio::time::sleep(Duration::from_millis(100)).await;

                tip_block_num = self
                    .get_block_num()
                    .await
                    .context("get tip block num from rpc")?;
            }
        }

        Ok(())
    }

    async fn initial_sync(&self) -> Result<u64> {
        let to_block = self
            .get_block_num()
            .await
            .context("get tip block num from rpc")?;

        let step: u64 = self.config.batch_size.get().try_into().unwrap();

        log::info!(
            "starting initial sync from {} to {}",
            self.config.from_block,
            to_block
        );

        let futs = (self.config.from_block..to_block)
            .step_by(self.config.batch_size.get())
            .map(|start_block| {
                let client = self.client.clone();

                async move {
                    let end_block = cmp::min(to_block, start_block + step);

                    log::debug!("ingesting {}-{}", start_block, end_block);

                    let req: RpcRequest = (start_block..end_block)
                        .map(|block_num| GetBlockByNumber(block_num.into()))
                        .collect::<Vec<_>>()
                        .into();

                    let resp = client.send(req).await.context("get block headers")?;

                    let blocks: Vec<Block<Transaction>> = resp.try_into().unwrap();

                    let req: RpcRequest = (start_block..end_block)
                        .map(|block_num| GetBlockReceipts(block_num.into()))
                        .collect::<Vec<_>>()
                        .into();

                    let resp = client.send(req).await.context("get block receipts")?;

                    let receipts: Vec<Vec<TransactionReceipt>> = resp.try_into().unwrap();

                    Ok::<_, Error>(BatchData {
                        blocks,
                        receipts,
                        from_block: start_block,
                        to_block: end_block,
                    })
                }
            });

        let mut data_stream = stream::iter(futs).buffered(self.config.concurrency_limit.get());
        while let Some(data) = data_stream.next().await {
            let data = data?;

            validate_batch_data(&data).context("validate data")?;

            if self.data_tx.send(data).await.is_err() {
                log::warn!("no one is listening so quitting ingest loop.");
                break;
            }
        }

        log::info!("finished initial sync");

        Ok(to_block)
    }

    async fn get_block_num(&self) -> Result<u64> {
        let to_block: BlockNumber = self
            .client
            .send(GetBlockNumber.into())
            .await
            .context("execute GetBlockNumber request")?
            .try_into_single()
            .unwrap();
        let to_block: u64 = to_block.into();

        Ok(to_block)
    }
}
