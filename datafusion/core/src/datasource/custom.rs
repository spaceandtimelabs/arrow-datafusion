use crate::datasource::TableProvider;
use crate::execution::context::{SessionState, TaskContext};
use crate::physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion_expr::{Expr, TableType};
use std::any::Any;

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use backtrace::Backtrace;
use datafusion_common::DataFusionError;
use datafusion_physical_expr::PhysicalSortExpr;
use crate::physical_plan::Partitioning::UnknownPartitioning;

/// Exists to pass table_type, path, and options from parsing to serialization
#[derive(Debug)]
pub struct CustomTable {
    /// File/Table type used for factory to create TableProvider
    table_type: String,
    /// Path used for factory to create TableProvider
    path: String,
    /// Options used for factory to create TableProvider
    options: HashMap<String, String>,
    // TableProvider that was created
    provider: Arc<dyn TableProvider>,
}

impl CustomTable {
    pub fn new(
        table_type: &str,
        path: &str,
        options: HashMap<String, String>,
        provider: Arc<dyn TableProvider>,
    ) -> Self {
        println!("CustomTable for {:?} as {:?}", provider, Backtrace::new());
        Self {
            table_type: table_type.to_string(),
            path: path.to_string(),
            options,
            provider,
        }
    }

    pub fn get_table_type(&self) -> String {
        self.table_type.clone()
    }

    pub fn get_path(&self) -> String {
        self.path.clone()
    }

    pub fn get_options(&self) -> HashMap<String, String> {
        self.options.clone()
    }

}

#[async_trait]
impl TableProvider for CustomTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        println!("schema for {:?} as {:?}", self.provider, Backtrace::new());
        self.provider.schema()
    }

    fn table_type(&self) -> TableType {
        println!("table_type for {:?} as {:?}", self.provider, Backtrace::new());
        self.provider.table_type()
    }

    async fn scan(
        &self,
        ctx: &SessionState,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        println!("scan for {:?} as {:?}", self.provider, Backtrace::new());
        let plan = self.provider.scan(ctx, projection, filters, limit).await?;
        let schema = self.schema();
        Ok(Arc::new(CustomPlan::new(&self.table_type, &self.path, &self.options, schema, plan)))
    }
}

#[derive(Debug)]
pub struct CustomPlan {
    table_type: String,
    url: String,
    options: HashMap<String, String>,
    schema: SchemaRef,
    plan: Arc<dyn ExecutionPlan>,
}

impl CustomPlan {
    pub fn new(
        table_type: &String,
        url: &String,
        options: &HashMap<String, String>,
        schema: SchemaRef,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Self {
        Self {
            table_type: table_type.clone(),
            url: url.clone(),
            options: options.clone(),
            schema,
            plan,
        }
    }

    pub fn table_type(&self) -> &String {
        &self.table_type
    }

    pub fn url(&self) -> &String {
        &self.url
    }

    pub fn options(&self) -> &HashMap<String, String> {
        &self.options
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl ExecutionPlan for CustomPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        todo!("CustomPlan::with_new_children")
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> datafusion_common::Result<SendableRecordBatchStream> {
        let table_type = self.table_type.to_lowercase();
        let env = context.runtime_env();
        let factory = env
            .table_factories
            .get(&table_type)
            .expect("TODO: Correct error for ExecutionPlan::execute");
        let table = factory.with_schema(
            self.schema().clone(),
            self.table_type.as_str(),
            self.url.as_str(),
            self.options.clone(),
        ).expect("TODO: Correct error for ExecutionPlan::execute");
        
        todo!("CustomPlan::execute")
    }

    fn statistics(&self) -> Statistics {
        todo!("CustomPlan::statistics")
    }
}
