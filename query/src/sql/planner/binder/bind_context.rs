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

use common_ast::ast::TableAlias;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::sql::common::IndexType;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::Scalar;

#[derive(Clone, PartialEq, Debug)]
pub struct ColumnBinding {
    // Table name of this `ColumnBinding` in current context
    pub table_name: Option<String>,
    // Column name of this `ColumnBinding` in current context
    pub column_name: String,
    // Column index of ColumnBinding
    pub index: IndexType,

    pub data_type: DataTypeImpl,

    // Scalar expression the `ColumnBinding` refers to(if exists).
    // For example, a column `b` can be generated by `SELECT a+1 AS b FROM t`.
    // Another example is aggregation. In a `GROUP BY` context, aggregate funtions
    // will be extracted and be added to `BindContext` as a `ColumnBinding`.
    pub scalar: Option<Box<Scalar>>,
}

/// `BindContext` stores all the free variables in a query and tracks the context of binding procedure.
#[derive(Clone, Default)]
pub struct BindContext {
    _parent: Option<Box<BindContext>>,
    columns: Vec<ColumnBinding>,

    /// The relational operator in current context
    pub expression: Option<SExpr>,

    /// Aggregation scalar expression
    pub agg_scalar_exprs: Option<Vec<Scalar>>,
}

impl BindContext {
    pub fn new() -> Self {
        Self::default()
    }

    fn new_with_parent(parent: Box<BindContext>) -> Self {
        BindContext {
            _parent: Some(parent),
            columns: vec![],
            expression: None,
            agg_scalar_exprs: None,
        }
    }

    /// Generate a new BindContext and take current BindContext as its parent.
    pub fn push(self) -> Self {
        Self::new_with_parent(Box::new(self))
    }

    /// Returns all column bindings in current scope.
    pub fn all_column_bindings(&self) -> &[ColumnBinding] {
        &self.columns
    }

    pub fn add_column_binding(&mut self, column_binding: ColumnBinding) {
        self.columns.push(column_binding);
    }

    /// Apply table alias like `SELECT * FROM t AS t1(a, b, c)`.
    /// This method will rename column bindings according to table alias.
    pub fn apply_table_alias(&mut self, original_name: &str, alias: &TableAlias) -> Result<()> {
        for column in self.columns.iter_mut() {
            if let Some(table_name) = &column.table_name {
                if table_name.as_str() == original_name {
                    column.table_name = Some(alias.name.to_string());
                }
            }
        }

        if alias.columns.len() > self.columns.len() {
            return Err(ErrorCode::SemanticError(format!(
                "table has {} columns available but {} columns specified",
                self.columns.len(),
                alias.columns.len()
            )));
        }
        for (index, column_name) in alias.columns.iter().map(ToString::to_string).enumerate() {
            self.columns[index].column_name = column_name;
        }
        Ok(())
    }

    /// Try to find a column binding with given table name and column name.
    /// This method will return error if the given names are ambiguous or invalid.
    pub fn resolve_column(&self, table: Option<String>, column: String) -> Result<ColumnBinding> {
        // TODO: lookup parent context to support correlated subquery
        let mut result = vec![];
        if let Some(table) = table {
            for column_binding in self.columns.iter() {
                if let Some(table_name) = &column_binding.table_name {
                    if table_name == &table && column_binding.column_name == column {
                        result.push(column_binding.clone());
                    }
                }
            }
        } else {
            for column_binding in self.columns.iter() {
                if column_binding.column_name.eq(&column) {
                    result.push(column_binding.clone());
                }
            }
        }

        if result.is_empty() {
            Err(ErrorCode::SemanticError(format!(
                "column \"{}\" doesn't exist",
                column
            )))
        } else if result.len() > 1 {
            Err(ErrorCode::SemanticError(format!(
                "column reference \"{}\" is ambiguous",
                column
            )))
        } else {
            Ok(result.remove(0))
        }
    }

    /// Get result columns of current context in order.
    /// For example, a query `SELECT b, a AS b FROM t` has `[(index_of(b), "b"), index_of(a), "b"]` as
    /// its result columns.
    ///
    /// This method is used to retrieve the physical representation of result set of
    /// a query.
    pub fn result_columns(&self) -> Vec<(IndexType, String)> {
        self.columns
            .iter()
            .map(|col| (col.index, col.column_name.clone()))
            .collect()
    }
}
