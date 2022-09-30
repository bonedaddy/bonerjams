use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub struct DbOpts {
    /// if Some, enable compression and set factor to this
    pub compression_factor: Option<i32>,
    /// if true, print profile stats when database is dropped
    pub debug: bool,
    pub mode: Option<DbMode>,
    pub path: String,
    /// size of system page cache in bytes
    pub system_page_cache: Option<u64>,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub enum DbMode {
    LowSpace,
    Fast,
}

impl Default for DbMode {
    fn default() -> Self {
        Self::Fast
    }
}

impl From<DbMode> for sled::Mode {
    fn from(conf: DbMode) -> Self {
        match conf {
            DbMode::LowSpace => sled::Mode::LowSpace,
            DbMode::Fast => sled::Mode::HighThroughput,
        }
    }
}

impl Default for DbOpts {
    fn default() -> Self {
        Self {
            path: "test_infos.db".to_string(),
            system_page_cache: None,
            compression_factor: None,
            mode: Default::default(),
            debug: false,
        }
    }
}

impl From<&DbOpts> for sled::Config {
    fn from(opts: &DbOpts) -> Self {
        let mut sled_config = sled::Config::new();
        sled_config = sled_config.path(opts.path.clone());
        if let Some(cache) = opts.system_page_cache.as_ref() {
            sled_config = sled_config.cache_capacity(*cache);
        }
        if let Some(compression) = opts.compression_factor.as_ref() {
            sled_config = sled_config.use_compression(true);
            sled_config = sled_config.compression_factor(*compression);
        }
        if let Some(mode) = opts.mode {
            sled_config = sled_config.mode(mode.into());
        }
        if opts.debug {
            sled_config = sled_config.print_profile_on_drop(true);
        }
        sled_config
    }
}
