// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_app::schema::CreateTableReply;
use common_meta_app::schema::TableInfo;
use common_planners::Extras;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use strum_macros::Display;
use strum_macros::EnumIter;
use strum_macros::EnumString;

use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::AsyncSource;
use crate::pipelines::new::processors::AsyncSourcer;
use crate::pipelines::new::NewPipe;
use crate::pipelines::new::NewPipeline;
use crate::sessions::QueryContext;
use crate::storages::github::RepoCommentsTable;
use crate::storages::github::RepoInfoTable;
use crate::storages::github::RepoIssuesTable;
use crate::storages::github::RepoPRsTable;
use crate::storages::github::RepoTableOptions;
use crate::storages::StorageContext;
use crate::storages::StorageDescription;
use crate::storages::Table;

#[derive(Debug, Clone, EnumIter, EnumString, Display)]
#[strum(serialize_all = "snake_case")]
pub enum GithubTableType {
    Comments,
    Info,
    Issues,
    PullRequests,
}

pub struct GithubTable {
    table_info: TableInfo,
    options: RepoTableOptions,
}

impl GithubTable {
    pub fn try_create(_ctx: StorageContext, table_info: TableInfo) -> Result<Box<dyn Table>> {
        let engine_options = table_info.engine_options();
        Ok(Box::new(GithubTable {
            options: engine_options.try_into()?,
            table_info,
        }))
    }

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: "GITHUB".to_string(),
            comment: "GITHUB Storage Engine".to_string(),
            ..Default::default()
        }
    }
}

#[async_trait::async_trait]
impl Table for GithubTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read_partitions(
        &self,
        _ctx: Arc<QueryContext>,
        _push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        Ok((Statistics::default(), vec![]))
    }

    async fn read(
        &self,
        _ctx: Arc<QueryContext>,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let table: Box<dyn GithubDataGetter> = self.options.clone().into();
        let arrays = table.get_data_from_github().await?;
        let block = DataBlock::create(self.table_info.schema(), arrays);

        Ok(Box::pin(DataBlockStream::create(
            self.table_info.schema(),
            None,
            vec![block],
        )))
    }

    fn read2(
        &self,
        ctx: Arc<QueryContext>,
        _: &ReadDataSourcePlan,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        let output = OutputPort::create();
        let options = self.options.clone();
        let schema = self.table_info.schema();
        pipeline.add_pipe(NewPipe::SimplePipe {
            inputs_port: vec![],
            outputs_port: vec![output.clone()],
            processors: vec![GithubSource::create(ctx, output, schema, options)?],
        });

        Ok(())
    }
}

#[async_trait::async_trait]
pub trait GithubDataGetter: Sync + Send {
    async fn get_data_from_github(&self) -> Result<Vec<ColumnRef>>;
}

#[async_trait::async_trait]
pub trait GithubTableCreater: Sync + Send {
    async fn create_table(&self, ctx: &StorageContext, tenant: &str) -> Result<CreateTableReply>;
}

impl From<RepoTableOptions> for Box<dyn GithubDataGetter> {
    fn from(options: RepoTableOptions) -> Self {
        match options.table_type {
            GithubTableType::Comments => Box::new(RepoCommentsTable { options }),
            GithubTableType::Info => Box::new(RepoInfoTable { options }),
            GithubTableType::Issues => Box::new(RepoIssuesTable { options }),
            GithubTableType::PullRequests => Box::new(RepoPRsTable { options }),
        }
    }
}

impl From<RepoTableOptions> for Box<dyn GithubTableCreater> {
    fn from(options: RepoTableOptions) -> Self {
        match options.table_type {
            GithubTableType::Comments => Box::new(RepoCommentsTable { options }),
            GithubTableType::Info => Box::new(RepoInfoTable { options }),
            GithubTableType::Issues => Box::new(RepoIssuesTable { options }),
            GithubTableType::PullRequests => Box::new(RepoPRsTable { options }),
        }
    }
}

struct GithubSource {
    finish: bool,
    schema: DataSchemaRef,
    options: RepoTableOptions,
}

impl GithubSource {
    pub fn create(
        ctx: Arc<QueryContext>,
        output: Arc<OutputPort>,
        schema: DataSchemaRef,
        options: RepoTableOptions,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx, output, GithubSource {
            schema,
            options,
            finish: false,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for GithubSource {
    const NAME: &'static str = "GithubSource";

    #[async_trait::unboxed_simple]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finish {
            return Ok(None);
        }

        self.finish = true;
        let table: Box<dyn GithubDataGetter> = self.options.clone().into();
        let arrays = table.get_data_from_github().await?;
        Ok(Some(DataBlock::create(self.schema.clone(), arrays)))
    }
}
