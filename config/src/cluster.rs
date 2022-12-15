use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct ClusterOpts {
    pub seeds: Vec<String>,
    /// this is the internal rpc service used by the cluster service
    pub cluster_rpc_endpoint: String,
    /// this is the endpoint that nodes in hte cluster will connect too
    pub cluster_node_endpoint: String,
    pub cluster_id: Option<String>,
    pub data_center: Option<String>,
    pub repair_interval: Option<String>,
}
