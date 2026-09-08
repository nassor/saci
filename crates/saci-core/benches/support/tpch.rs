//! The TPC-H `Lineitem` shape shared by the `tpch_q1` and `tpch_q6` benchmarks.
//!
//! Only the schema is shared. Each benchmark keeps its own row generator: they
//! draw from the LCG in a different order and over different date ranges, so
//! unifying them would change the inputs.
#![allow(dead_code)]

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use saci_core::component::Component;
use serde::{Deserialize, Serialize};

/// Lineitem component: the TPC-H subset both queries read.
#[derive(Serialize, Deserialize, Clone)]
pub struct Lineitem {
    pub l_orderkey: i64,
    pub l_partkey: i64,
    pub l_suppkey: i64,
    pub l_linenumber: i32,
    pub l_quantity: f64,
    pub l_extendedprice: f64,
    pub l_discount: f64,
    pub l_tax: f64,
    pub l_returnflag: u8,
    pub l_linestatus: u8,
    pub l_shipdate: i32,
    pub l_commitdate: i32,
}

impl Component for Lineitem {
    fn name() -> &'static str {
        "Lineitem"
    }
    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_partkey", DataType::Int64, false),
            Field::new("l_suppkey", DataType::Int64, false),
            Field::new("l_linenumber", DataType::Int32, false),
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_extendedprice", DataType::Float64, false),
            Field::new("l_discount", DataType::Float64, false),
            Field::new("l_tax", DataType::Float64, false),
            Field::new("l_returnflag", DataType::UInt8, false),
            Field::new("l_linestatus", DataType::UInt8, false),
            Field::new("l_shipdate", DataType::Int32, false),
            Field::new("l_commitdate", DataType::Int32, false),
        ]))
    }
}
