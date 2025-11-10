use bincode::{Decode, Encode};
use directories::ProjectDirs;
use serde::{Serialize, Deserialize};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::time::Duration;
use ebi_filesystem::{config::Config as FsConfig, file};
use anyhow::{Context, Result};

pub struct Config {
    pub inner_conf: InnerConfig,
    pub project_dir: ProjectDirs,
}

#[derive(Serialize, Default, Deserialize, Encode, Decode)]
pub struct InnerConfig {
    pub data_dir: Option<PathBuf>,
    pub refresh_interval: Option<Duration>,
}

impl Config {
    pub fn new(project_dir: ProjectDirs) -> Result<Self> {
        let config_path = project_dir.config_dir().join("config.toml");
        let inner_conf = if std::fs::exists(&config_path)? {
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .create(false)
                .open(&config_path)
                .with_context(|| format!("Could not have opened config at {}", &config_path.display()))?;

            let mut file_bytes = Vec::new();
            file.read_to_end(&mut file_bytes)?;

            toml::from_slice(&file_bytes)?
        } else {
            let mut file = std::fs::File::create(&config_path)
                .with_context(|| format!("Could not have create default config at {}", &config_path.display()))?;
            let inner_conf = InnerConfig::default();

            let bincode_config = bincode::config::standard();
            bincode::encode_into_std_write(&inner_conf, &mut file, bincode_config)?;
            inner_conf
        };

        Ok(Self {
            inner_conf,
            project_dir
        })
    }


    pub fn setup_fs_config(&self) -> FsConfig {
        let database_dir = match &self.inner_conf.data_dir {
            Some(p) => p.to_path_buf(),
            None => self.project_dir.data_dir().to_path_buf()
        };

        let refresh_interval = match self.inner_conf.refresh_interval {
            Some(i) => i,
            None => Duration::from_millis(50),
        };

        FsConfig {
            database_dir,
            refresh_interval,
        }
    }
}
