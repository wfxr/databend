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
use octocrab::params;

use super::github_table::GithubTableCreater;
use crate::storages::github::create_github_client;
use crate::storages::github::GithubDataGetter;
use crate::storages::github::RepoTableOptions;
use crate::storages::StorageContext;

const NUMBER: &str = "number";
const TITLE: &str = "title";
const STATE: &str = "state";
const USER: &str = "user";
const LABELS: &str = "labels";
const ASSIGNESS: &str = "assigness";
const COMMENTS: &str = "comments";
const CREATED_AT: &str = "created_at";
const UPDATED_AT: &str = "updated_at";
const CLOSED_AT: &str = "closed_at";

pub struct RepoIssuesTable {
    options: RepoTableOptions,
}

impl RepoIssuesTable {
    fn schema() -> Arc<DataSchema> {
        let fields = vec![
            DataField::new(NUMBER, i64::to_data_type()),
            DataField::new(TITLE, Vu8::to_data_type()),
            DataField::new(STATE, Vu8::to_data_type()),
            DataField::new(USER, Vu8::to_data_type()),
            DataField::new(LABELS, Vu8::to_data_type()),
            DataField::new(ASSIGNESS, Vu8::to_data_type()),
            DataField::new(COMMENTS, u32::to_data_type()),
            DataField::new(CREATED_AT, TimestampType::new_impl(0)),
            DataField::new(UPDATED_AT, TimestampType::new_impl(0)),
            DataField::new_nullable(CLOSED_AT, TimestampType::new_impl(0)),
        ];

        Arc::new(DataSchema::new(fields))
    }
}

#[async_trait::async_trait]
impl GithubDataGetter for RepoIssuesTable {
    async fn get_data_from_github(&self) -> Result<Vec<ColumnRef>> {
        // init array
        let mut issue_numer_array: Vec<i64> = Vec::new();
        let mut title_array: Vec<Vec<u8>> = Vec::new();
        let mut state_array: Vec<Vec<u8>> = Vec::new();
        let mut user_array: Vec<Vec<u8>> = Vec::new();
        let mut labels_array: Vec<Vec<u8>> = Vec::new();
        let mut assigness_array: Vec<Vec<u8>> = Vec::new();
        let mut comments_number_array: Vec<u32> = Vec::new();
        let mut created_at_array: Vec<u32> = Vec::new();
        let mut updated_at_array: Vec<u32> = Vec::new();
        let mut closed_at_array: Vec<Option<u32>> = Vec::new();

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
            .list()
            // Optional Parameters
            .state(params::State::All)
            .per_page(100)
            .send()
            .await?;

        let issues = instance.all_pages::<models::issues::Issue>(page).await?;
        for issue in issues {
            issue_numer_array.push(issue.number);
            title_array.push(issue.title.clone().into());
            state_array.push(issue.state.clone().into());
            user_array.push(issue.user.login.clone().into());
            let mut labels_str = issue.labels.iter().fold(Vec::new(), |mut content, label| {
                content.extend_from_slice(label.name.clone().as_bytes());
                content.push(b',');
                content
            });
            labels_str.pop();
            labels_array.push(labels_str);
            let mut assigness_str = issue
                .assignees
                .iter()
                .fold(Vec::new(), |mut content, user| {
                    content.extend_from_slice(user.login.clone().as_bytes());
                    content.push(b',');
                    content
                });
            assigness_str.pop();
            assigness_array.push(assigness_str);
            comments_number_array.push(issue.comments);
            let created_at = (issue.created_at.timestamp_millis() / 1000) as u32;
            created_at_array.push(created_at);
            let updated_at = (issue.updated_at.timestamp_millis() / 1000) as u32;
            updated_at_array.push(updated_at);
            let closed_at = issue
                .closed_at
                .map(|closed_at| (closed_at.timestamp_millis() / 1000) as u32);
            closed_at_array.push(closed_at);
        }

        Ok(vec![
            Series::from_data(issue_numer_array),
            Series::from_data(title_array),
            Series::from_data(state_array),
            Series::from_data(user_array),
            Series::from_data(labels_array),
            Series::from_data(assigness_array),
            Series::from_data(comments_number_array),
            Series::from_data(created_at_array),
            Series::from_data(updated_at_array),
            Series::from_data(closed_at_array),
        ])
    }
}

#[async_trait::async_trait]
impl GithubTableCreater for RepoIssuesTable {
    async fn create_table(&self, ctx: &StorageContext, tenant: &str) -> Result<CreateTableReply> {
        let opt = self.options.clone();
        let req = CreateTableReq {
            if_not_exists: false,
            name_ident: TableNameIdent {
                tenant: tenant.to_string(),
                db_name: opt.owner.clone(),
                table_name: format!("{}_{}", opt.repo.clone(), "issues"),
            },
            table_meta: TableMeta {
                schema: RepoIssuesTable::schema(),
                engine: "GITHUB".into(),
                engine_options: opt.into(),
                ..Default::default()
            },
        };
        Ok(ctx.meta.create_table(req).await?)
    }
}
