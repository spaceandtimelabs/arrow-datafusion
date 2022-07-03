use std::collections::HashSet;
use crate::{utils, OptimizerConfig, OptimizerRule};
use datafusion_common::{Column, DataFusionError};
use datafusion_expr::logical_plan::{Filter, JoinType, Subquery};
use datafusion_expr::{combine_filters, Expr, LogicalPlan, LogicalPlanBuilder};
use std::sync::Arc;
use crate::utils::{find_join_exprs, get_id, split_conjunction};

/// Optimizer rule for rewriting subquery filters to joins
#[derive(Default)]
pub struct DecorrelateScalarSubquery {}

impl DecorrelateScalarSubquery {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for DecorrelateScalarSubquery {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &OptimizerConfig,
    ) -> datafusion_common::Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(Filter { predicate, input }) => {
                // Apply optimizer rule to current input
                let optimized_input = self.optimize(input, optimizer_config)?;

                let (subqueries, others) = extract_subquery_exprs(predicate);
                let subquery_expr = match subqueries.as_slice() {
                    [it] => it,
                    _ => {
                        let optimized_plan = LogicalPlan::Filter(Filter {
                            predicate: predicate.clone(),
                            input: Arc::new(optimized_input),
                        });
                        return Ok(optimized_plan); // TODO: >1 subquery
                    }
                };
                let others: Vec<_> = others.iter().map(|it| (*it).clone()).collect();

                optimize_scalar(plan, &subquery_expr, &optimized_input, &others)
            }
            _ => {
                // Apply the optimization to all inputs of the plan
                utils::optimize_children(self, plan, optimizer_config)
            }
        }
    }

    fn name(&self) -> &str {
        "decorrelate_scalar_subquery"
    }
}

fn optimize_scalar(
    plan: &LogicalPlan,
    subquery_expr: &Expr,
    input: &LogicalPlan,
    outer_others: &[Expr],
) -> datafusion_common::Result<LogicalPlan> {
    let subqueries = extract_subqueries(subquery_expr);
    let subquery = match subqueries.as_slice() {
        [it] => it,
        _ => return Ok(plan.clone())
    };

    let proj = match &*subquery.subquery {
        LogicalPlan::Projection(it) => it,
        _ => return Ok(plan.clone())
    };
    let proj = match proj.expr.as_slice() {
        [it] => it,
        _ => return Ok(plan.clone()) // scalar subquery means only 1 expr
    };
    let proj = Expr::Alias(Box::new(proj.clone()), "__value".to_string());

    // Only operate if there is one input
    let sub_inputs = subquery.subquery.inputs();
    let sub_input = match sub_inputs.as_slice() {
        [it] => it,
        _ => return Ok(plan.clone())
    };

    // Only operate on subqueries that are trying to filter on an expression from an outer query
    let aggr = match sub_input {
        LogicalPlan::Aggregate(a) => a,
        _ => return Ok(plan.clone())
    };
    let filter = match &*aggr.input {
        LogicalPlan::Filter(f) => f,
        _ => return Ok(plan.clone())
    };

    // split into filters
    let mut filters = vec![];
    utils::split_conjunction(&filter.predicate, &mut filters);

    // get names of fields TODO: Must fully qualify these!
    let fields: HashSet<_> = filter.input
        .schema()
        .fields()
        .iter()
        .map(|f| f.name())
        .collect();
    println!("{:?}", fields);

    // Grab column names to join on
    let (cols, others) = find_join_exprs(filters, &fields);
    if cols.is_empty() {
        return Ok(plan.clone()); // no joins found
    }

    // Only operate if one column is present and the other closed upon from outside scope
    let r_alias = format!("__sq_{}", get_id());
    let l_col: Vec<_> = cols
        .iter()
        .map(|it| &it.0)
        .map(|it| Column::from(it.as_str()))
        .collect();
    let r_col: Vec<_> = cols
        .iter()
        .map(|it| &it.1)
        .map(|it| Column::from(it.as_str()))
        .collect();
    let group_by: Vec<_> = r_col.iter().map(|it| Expr::Column(it.clone())).collect();

    // build right side of join - the thing the subquery was querying
    let right = LogicalPlanBuilder::from((*filter.input).clone());
    let right = if let Some(expr) = combine_filters(&others) {
        right.filter(expr)? // if the subquery had additional expressions, restore them
    } else {
        right
    };
    let proj: Vec<_> = group_by.iter().cloned()
        .chain(vec![proj].iter().cloned()).collect();
    let right = right
        .aggregate(group_by.clone(), aggr.aggr_expr.clone())?
        .project_with_alias(proj, Some(r_alias.clone()))?
        .build()?;

    // qualify the join columns for outside the subquery
    let r_col: Vec<_> = cols
        .iter()
        .map(|it| &it.1)
        .map(|it| {
            Column { relation: Some(r_alias.clone()), name: it.clone() }
        })
        .collect();
    let join_keys = (l_col, r_col);
    println!("Joining:\n{}\nto:\n{}\non{:?}", right.display_indent(), input.display_indent(), join_keys);

    // join our sub query into the main plan
    let new_plan = LogicalPlanBuilder::from(input.clone())
        .join(&right, JoinType::Inner, join_keys, None)?;
    let new_plan = if let Some(expr) = combine_filters(outer_others) {
        new_plan.filter(expr)? // if the main query had additional expressions, restore them
    } else {
        new_plan
    };

    // restore conditions
    let new_plan = match subquery_expr {
        Expr::BinaryExpr { left, op, right } => {
            match &**right {
                Expr::ScalarSubquery(subquery) => {
                    let right = Box::new(Expr::Column(Column {
                        relation: Some(r_alias),
                        name: "__value".to_string(),
                    }));
                    let expr = Expr::BinaryExpr { left: left.clone(), op: op.clone(), right };
                    new_plan.filter(expr)?
                }
                _ => return Err(DataFusionError::Plan("Not a scalar subquery!".to_string()))
            }
        }
        _ => return Err(DataFusionError::Plan("Not a scalar subquery!".to_string()))
    };

    let new_plan = new_plan.build()?;
    println!("{}", new_plan.display_indent());
    Ok(new_plan)
}

pub fn extract_subquery_exprs(predicate: &Expr) -> (Vec<&Expr>, Vec<&Expr>) {
    let mut filters = vec![];
    split_conjunction(predicate, &mut filters);

    let (subqueries, others): (Vec<_>, Vec<_>) = filters.iter()
        .partition(|expr| {
            match expr {
                Expr::BinaryExpr { left, op: _, right } => {
                    let l_query = match &**left {
                        Expr::ScalarSubquery(subquery) => {
                            Some(subquery.clone())
                        }
                        _ => None,
                    };
                    let r_query = match &**right {
                        Expr::ScalarSubquery(subquery) => {
                            Some(subquery.clone())
                        }
                        _ => None,
                    };
                    if l_query.is_some() && r_query.is_some() {
                        return false; // TODO: (subquery A) = (subquery B)
                    }
                    match l_query {
                        Some(_) => return true,
                        _ => {}
                    }
                    match r_query {
                        Some(_) => return true,
                        _ => {}
                    }
                    false
                }
                _ => false
            }
        });
    (subqueries, others)
}

pub fn extract_subqueries(predicate: &Expr) -> Vec<Subquery> {
    let mut filters = vec![];
    split_conjunction(predicate, &mut filters);

    let subqueries = filters.iter()
        .fold(vec![], |mut acc, expr| {
            match expr {
                Expr::BinaryExpr { left, op: _, right } => {
                    vec![&**left, &**right].iter().for_each(|it| {
                        match it {
                            Expr::ScalarSubquery(subquery) => {
                                acc.push(subquery.clone());
                            }
                            _ => {}
                        };
                    })
                }
                _ => {}
            }
            acc
        });
    return subqueries;
}
