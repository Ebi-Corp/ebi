use serde::{Serialize, Deserialize};
use std::time::Duration;
use std::path::PathBuf;


#[derive(Serialize, Deserialize)]
pub struct Config {
    pub database_dir: PathBuf,
    pub refresh_interval: Duration,
}
