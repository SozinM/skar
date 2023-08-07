use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use anyhow::{Context, Result};
use sbbf_rs_safe::Filter as SbbfFilter;
use skar_format::Address;
use tokio::sync::mpsc;
use wyhash::wyhash;

use crate::{
    config::QueryConfig,
    db::{BlockRange, FolderIndexIterator},
    state::State,
    types::{LogSelection, Query, QueryResult, QueryResultData, TransactionSelection},
};

use super::{
    data_provider::{InMemDataProvider, ParquetDataProvider},
    execution::execute_query,
};

pub struct Handler {
    state: Arc<State>,
    cfg: QueryConfig,
    parquet_path: PathBuf,
}

impl Handler {
    pub fn new(cfg: QueryConfig, state: Arc<State>, parquet_path: &Path) -> Self {
        Self {
            state,
            cfg,
            parquet_path: parquet_path.to_owned(),
        }
    }

    pub async fn archive_height(&self) -> Result<Option<u64>> {
        let to_block = self.state.in_mem.load().to_block;
        if to_block > 0 {
            return Ok(Some(to_block - 1));
        }

        let next_block_num = self
            .state
            .db
            .next_block_num()
            .await
            .context("get next block num from db")?;

        if next_block_num > 0 {
            Ok(Some(next_block_num - 1))
        } else {
            Ok(None)
        }
    }

    pub fn handle(self: Arc<Self>, query: Query) -> Result<mpsc::Receiver<Result<QueryResult>>> {
        let handler = self.clone();
        let (tx, rx) = mpsc::channel(1);

        let folder_index_iterator = self
            .state
            .db
            .iterate_folder_indices(BlockRange(
                query.from_block,
                query.to_block.unwrap_or(u64::MAX),
            ))
            .context("start folder index iterator")?;

        tokio::task::spawn_blocking(move || {
            let iter = QueryResultIterator {
                finished: false,
                start_time: Instant::now(),
                handler,
                query,
                folder_index_iterator,
            };

            for res in iter {
                let is_err = res.is_err();
                if tx.blocking_send(res).is_err() {
                    break;
                }
                if is_err {
                    break;
                }
            }
        });

        Ok(rx)
    }
}

pub struct QueryResultIterator {
    finished: bool,
    start_time: Instant,
    handler: Arc<Handler>,
    query: Query,
    folder_index_iterator: FolderIndexIterator,
}

impl Iterator for QueryResultIterator {
    type Item = Result<QueryResult>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        if self.start_time.elapsed().as_millis() >= self.handler.cfg.time_limit_ms as u128 {
            self.finished = true;
            return None;
        }

        let folder_index = match self.folder_index_iterator.next() {
            Some(folder_index) => folder_index,
            None => {
                self.finished = true;

                let in_mem = self.handler.state.in_mem.load();

                if let Some(to_block) = self.query.to_block {
                    if to_block <= in_mem.from_block {
                        return None;
                    }
                }

                if self.query.from_block >= in_mem.to_block {
                    return None;
                }

                let data_provider = InMemDataProvider { in_mem: &in_mem };

                let query_res = execute_query(&data_provider, &self.query)
                    .map(|data| QueryResult {
                        data,
                        next_block: in_mem.to_block,
                    })
                    .context("execute in memory query");

                return Some(query_res);
            }
        };

        let folder_index = match folder_index {
            Ok(folder_index) => folder_index,
            Err(e) => return Some(Err(e.context("failed to read folder index"))),
        };

        let pruned_query = prune_query(&self.query, folder_index.address_filter.0);

        if pruned_query.logs.is_empty()
            && pruned_query.transactions.is_empty()
            && !pruned_query.include_all_blocks
        {
            return Some(Ok(QueryResult {
                data: QueryResultData::default(),
                next_block: folder_index.block_range.1,
            }));
        }

        let rg_index = match self
            .folder_index_iterator
            .read_row_group_index(folder_index.row_group_index_offset)
        {
            Ok(rg_index) => rg_index,
            Err(e) => return Some(Err(e.context("read row group index"))),
        };

        let mut path = self.handler.parquet_path.clone();
        path.push(format!(
            "{}-{}",
            folder_index.block_range.0, folder_index.block_range.1
        ));

        let data_provider = ParquetDataProvider { path, rg_index };

        let query_result = execute_query(&data_provider, &pruned_query).map(|data| QueryResult {
            data,
            next_block: folder_index.block_range.1,
        });

        Some(query_result)
    }
}

fn prune_query(query: &Query, filter: SbbfFilter) -> Query {
    let prune_addrs = |addrs: Vec<Address>| -> Option<Vec<Address>> {
        if !addrs.is_empty() {
            let out = addrs
                .into_iter()
                .filter(|addr| filter.contains_hash(wyhash(addr.as_slice(), 0)))
                .collect::<Vec<_>>();

            if out.is_empty() {
                None
            } else {
                Some(out)
            }
        } else {
            Some(Default::default())
        }
    };

    Query {
        logs: query
            .logs
            .iter()
            .cloned()
            .filter_map(|selection| {
                let address = prune_addrs(selection.address)?;
                Some(LogSelection {
                    address,
                    ..selection
                })
            })
            .collect(),
        transactions: query
            .transactions
            .iter()
            .cloned()
            .filter_map(|selection| {
                let from = prune_addrs(selection.from)?;
                let to = prune_addrs(selection.to)?;
                Some(TransactionSelection {
                    from,
                    to,
                    ..selection
                })
            })
            .collect(),
        from_block: query.from_block,
        to_block: query.to_block,
        field_selection: query.field_selection.clone(),
        include_all_blocks: query.include_all_blocks,
    }
}
