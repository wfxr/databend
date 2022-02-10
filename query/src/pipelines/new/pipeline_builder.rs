// Copyright 2022 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::{AggregatorFinalPlan, AggregatorPartialPlan, AlterUDFPlan, AlterUserPlan, BroadcastPlan, CopyPlan, CreateDatabasePlan, CreateTablePlan, CreateUDFPlan, CreateUserPlan, CreateUserStagePlan, DescribeStagePlan, DescribeTablePlan, DropDatabasePlan, DropTablePlan, DropUDFPlan, DropUserPlan, DropUserStagePlan, EmptyPlan, ExplainPlan, Expression, ExpressionPlan, FilterPlan, GrantPrivilegePlan, HavingPlan, InsertPlan, KillPlan, LimitByPlan, LimitPlan, OptimizeTablePlan, ProjectionPlan, RemotePlan, RevokePrivilegePlan, SelectPlan, SettingPlan, ShowCreateDatabasePlan, ShowCreateTablePlan, ShowGrantsPlan, SinkPlan, SortPlan, StagePlan, SubQueriesSetPlan, TruncateTablePlan, UseDatabasePlan, UseTenantPlan};
use common_planners::PlanNode;
use common_planners::PlanVisitor;
use common_planners::ReadDataSourcePlan;

use crate::pipelines::new::pipe::SourcePipeBuilder;
use crate::pipelines::new::pipe::TransformPipeBuilder;
use crate::pipelines::new::pipeline::NewPipeline;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::{AggregatorParams, AggregatorTransformParams, ExpressionTransform, TableSource, TransformAggregator};
use crate::pipelines::new::processors::TransformFilter;
use crate::sessions::QueryContext;

pub struct QueryPipelineBuilder {
    ctx: Arc<QueryContext>,
    pipeline: NewPipeline,
}

impl QueryPipelineBuilder {
    pub fn create(ctx: Arc<QueryContext>) -> QueryPipelineBuilder {
        QueryPipelineBuilder {
            ctx,
            pipeline: NewPipeline::create(),
        }
    }

    pub fn finalize(mut self, plan: &SelectPlan) -> Result<NewPipeline> {
        self.visit_select(plan)?;
        Ok(self.pipeline)
    }
}

impl PlanVisitor for QueryPipelineBuilder {
    fn visit_plan_node(&mut self, node: &PlanNode) -> Result<()> {
        match node {
            PlanNode::Projection(n) => self.visit_projection(n),
            PlanNode::Expression(n) => self.visit_expression(n),
            PlanNode::AggregatorPartial(n) => self.visit_aggregate_partial(n),
            PlanNode::AggregatorFinal(n) => self.visit_aggregate_final(n),
            PlanNode::Filter(n) => self.visit_filter(n),
            PlanNode::Having(n) => self.visit_having(n),
            PlanNode::Sort(n) => self.visit_sort(n),
            PlanNode::Limit(n) => self.visit_limit(n),
            PlanNode::LimitBy(n) => self.visit_limit_by(n),
            PlanNode::ReadSource(n) => self.visit_read_data_source(n),
            PlanNode::Select(n) => self.visit_select(n),
            PlanNode::Explain(n) => self.visit_explain(n),
            _ => Err(ErrorCode::UnImplement("")),
        }
    }

    fn visit_expression(&mut self, plan: &ExpressionPlan) -> Result<()> {
        self.visit_plan_node(&plan.input)?;

        self.pipeline.add_transform(|transform_input_port, transform_output_port| {
            ExpressionTransform::try_create(
                transform_input_port,
                transform_output_port,
                plan.input.schema(),
                plan.schema(),
                plan.exprs.to_owned(),
            )
        })
    }

    fn visit_aggregate_final(&mut self, plan: &AggregatorFinalPlan) -> Result<()> {
        self.visit_plan_node(&plan.input)?;

        self.pipeline.resize(1)?;
        let aggregator_params = AggregatorParams::try_create_final(plan)?;
        self.pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformAggregator::try_create_final(
                transform_input_port.clone(),
                transform_output_port.clone(),
                AggregatorTransformParams::try_create(
                    transform_input_port,
                    transform_output_port,
                    &aggregator_params,
                )?,
            )
        })
    }

    fn visit_aggregate_partial(&mut self, plan: &AggregatorPartialPlan) -> Result<()> {
        self.visit_plan_node(&plan.input)?;

        let aggregator_params = AggregatorParams::try_create_partial(plan)?;
        self.pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformAggregator::try_create_partial(
                transform_input_port.clone(),
                transform_output_port.clone(),
                AggregatorTransformParams::try_create(
                    transform_input_port,
                    transform_output_port,
                    &aggregator_params,
                )?,
            )
        })
    }

    fn visit_filter(&mut self, plan: &FilterPlan) -> Result<()> {
        self.visit_plan_node(&plan.input)?;

        self.pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformFilter::try_create(
                plan.schema(),
                plan.predicate.clone(),
                transform_input_port,
                transform_output_port,
            )
        })
    }

    fn visit_read_data_source(&mut self, plan: &ReadDataSourcePlan) -> Result<()> {
        // Bind plan partitions to context.
        self.ctx.try_set_partitions(plan.parts.clone())?;
        let table = self.ctx.build_table_from_source_plan(plan)?;
        table.read2(self.ctx.clone(), plan, &mut self.pipeline)
    }
}
