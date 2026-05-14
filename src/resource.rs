use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::CustomResource;
use kube_operator_util::status::{GetStatus, Status};
use kube_operator_util::util::GetObjectMeta;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    kind = "KafkaTopic",
    group = "pincette.net",
    version = "v1",
    namespaced,
    category = "controllers",
    shortname = "kt",
    printcolumn = r#"{"name":"Health", "type":"string", "jsonPath":".status.health.status"}"#,
    printcolumn = r#"{"name":"Phase", "type":"string", "jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Age", "type":"date", "jsonPath":".metadata.creationTimestamp"}"#
)]
#[kube(status = "Status")]
#[serde(rename_all = "camelCase")]
pub struct KafkaTopicSpec {
    pub max_message_bytes: Option<u64>,
    pub name: Option<String>,
    pub partitions: Option<u16>,
    pub replication_factor: Option<u16>,
    pub retention_bytes: Option<u64>,
    pub retention_milliseconds: Option<u64>,
}

impl GetObjectMeta for KafkaTopic {
    fn object_meta(&self) -> &ObjectMeta {
        &self.metadata
    }
}

impl GetStatus for KafkaTopic {
    fn status(&self) -> Option<&Status> {
        self.status.as_ref()
    }
}
