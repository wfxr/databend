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

use std::sync::Arc;

use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_app::schema::CreateTableReply;
use common_meta_app::schema::CreateTableReq;
use common_meta_app::schema::TableMeta;
use common_meta_app::schema::TableNameIdent;
use octocrab::models;

use super::github_table::GithubTableCreater;
use crate::storages::github::create_github_client;
use crate::storages::github::GithubDataGetter;
use crate::storages::github::RepoTableOptions;
use crate::storages::StorageContext;

const COMMENT_ID: &str = "comment_id";
const USER: &str = "user";
const BODY: &str = "body";

pub struct RepoCommentsTable {
    options: RepoTableOptions,
}

impl RepoCommentsTable {
    fn schema() -> Arc<DataSchema> {
        let fields = vec![
            DataField::new(COMMENT_ID, u64::to_data_type()),
            DataField::new(USER, Vu8::to_data_type()),
            DataField::new(BODY, Vu8::to_data_type()),
        ];

        Arc::new(DataSchema::new(fields))
    }
}

#[async_trait::async_trait]
impl GithubDataGetter for RepoCommentsTable {
    async fn get_data_from_github(&self) -> Result<Vec<ColumnRef>> {
        // init array
        let mut id_array: Vec<u64> = Vec::new();
        let mut user_array: Vec<Vec<u8>> = Vec::new();
        let mut body_array: Vec<Vec<u8>> = Vec::new();

        let RepoTableOptions {
            ref repo,
            ref owner,
            ref token,
            ..
        } = self.options;
        let instance = create_github_client(token)?;

        #[allow(unused_mut)]
        let mut page = instance
            .issues(owner, repo)
            .list_issue_comments()
            // Optional Parameters
            .per_page(100)
            .send()
            .await?;

        let comments = instance.all_pages::<models::issues::Comment>(page).await?;
        for comment in comments {
            id_array.push(comment.id.into_inner());
            user_array.push(comment.user.login.clone().into());
            body_array.push(
                comment
                    .body
                    .unwrap_or_else(|| "".to_string())
                    .as_bytes()
                    .to_vec(),
            )
        }

        Ok(vec![
            Series::from_data(id_array),
            Series::from_data(user_array),
            Series::from_data(body_array),
        ])
    }
}

#[async_trait::async_trait]
impl GithubTableCreater for RepoCommentsTable {
    async fn create_table(&self, ctx: &StorageContext, tenant: &str) -> Result<CreateTableReply> {
        let opt = self.options.clone();
        let req = CreateTableReq {
            if_not_exists: false,
            name_ident: TableNameIdent {
                tenant: tenant.to_string(),
                db_name: opt.owner.clone(),
                table_name: format!("{}_{}", opt.repo.clone(), "comments"),
            },
            table_meta: TableMeta {
                schema: RepoCommentsTable::schema(),
                engine: "GITHUB".into(),
                engine_options: opt.into(),
                ..Default::default()
            },
        };
        Ok(ctx.meta.create_table(req).await?)
    }
}
