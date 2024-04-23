use sea_orm::entity::prelude::*;
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq, DeriveEntityModel, Deserialize)]
#[sea_orm(table_name = "event")]
pub struct Model {
    #[sea_orm(primary_key)]
    #[serde(default)]
    pub id: i32,
    pub timestamp: String,
    pub bid: String,
    pub ask: String,
    pub bid_vol: String,
    pub ask_vol: String,
}

#[derive(Debug, Copy, Clone, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
