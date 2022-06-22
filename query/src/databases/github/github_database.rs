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

use common_exception::Result;
use common_meta_app::schema::DatabaseInfo;
use common_tracing::tracing;
use itertools::Itertools;
use octocrab::params;
use strum::IntoEnumIterator;

use crate::databases::Database;
use crate::databases::DatabaseContext;
use crate::storages::github::create_github_client;
use crate::storages::github::GithubTableCreater;
use crate::storages::github::GithubTableType;
use crate::storages::github::RepoTableOptions;
use crate::storages::StorageContext;

#[derive(Clone)]
pub struct GithubDatabase {
    ctx: DatabaseContext,
    db_info: DatabaseInfo,
}

impl GithubDatabase {
    pub fn try_create(ctx: DatabaseContext, db_info: DatabaseInfo) -> Result<Box<dyn Database>> {
        Ok(Box::new(Self { ctx, db_info }))
    }
}

#[async_trait::async_trait]
impl Database for GithubDatabase {
    fn name(&self) -> &str {
        &self.db_info.name_ident.db_name
    }

    fn get_db_info(&self) -> &DatabaseInfo {
        &self.db_info
    }

    async fn init_database(&self, tenant: &str) -> Result<()> {
        let token = self
            .get_db_info()
            .meta
            .engine_options
            .get("token")
            .cloned()
            .unwrap_or_default();
        // 1. get all repos in this organization
        let instance = create_github_client(&token)?;
        let repos = instance
            .orgs(self.name())
            .list_repos()
            .repo_type(params::repos::Type::Sources)
            .sort(params::repos::Sort::Pushed)
            .direction(params::Direction::Descending)
            .per_page(100)
            .send()
            .await?;

        let storage_ctx = StorageContext {
            meta: self.ctx.meta.clone(),
            in_memory_data: self.ctx.in_memory_data.clone(),
        };
        let options = repos
            .items
            .iter()
            .cartesian_product(GithubTableType::iter())
            .map(|(repo, table_type)| RepoTableOptions {
                owner: self.name().to_string(),
                repo: repo.name.clone(),
                token: token.clone(),
                table_type,
            });
        // 2. create all tables in need
        for opt in options {
            tracing::error!("creating table {} for repo {}", opt.table_type, opt.repo);
            let creator: Box<dyn GithubTableCreater> = opt.into();
            creator.create_table(&storage_ctx, tenant).await?;
        }

        Ok(())
    }
}
