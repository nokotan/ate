#[allow(unused_imports)]
use log::{info, warn, debug, error};
use serde::*;

use super::*;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Group {
    pub name: String,
    pub gid: u32,
    pub roles: Vec<Role>,
}