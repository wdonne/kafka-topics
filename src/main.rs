use futures::StreamExt;
use std::collections::HashMap;
use std::env;
mod resource;

use anyhow::Result;
use config::{Config, ConfigError};
use futures::future::join_all;
use k8s_openapi::api::core::v1::ObjectReference;
use kube::core::object::HasStatus;
use kube::runtime::controller::Action;
use kube::runtime::events::{Recorder, Reporter};
use kube::{Api, Client, Resource};
use kube_operator_util::status::{is_not_ready, patch_status};
use kube_operator_util::util::{
    DEFAULT_BACK_OFF, DEFAULT_RECONCILIATION_INTERVAL, GetObjectMeta, error_event,
    has_done_update_since_at_least, report_reconciliation, serial_controller, should_reconcile,
    watch_namespaced,
};
use log::info;
use rdkafka::ClientConfig;
use rdkafka::admin::{
    AdminClient, AdminOptions, AlterConfig, ConfigEntry, NewTopic, ResourceSpecifier,
    TopicReplication,
};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use resource::KafkaTopic;
use rustls::crypto::ring::default_provider;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::time::sleep;

const CONFIG_FILE: &str = "CONFIG_FILE";
const CONTROLLER: &str = "kafka-topics";
const DEFAULT_CONFIG_FILE: &str = "conf/application";
const DEFAULT_PARTITIONS: &str = "default-partitions";
const DEFAULT_REPLICATION_FACTOR: &str = "default-replication-factor";
const KAFKA: &str = "kafka";
const MAX_MESSAGE_BYTES: &str = "max.message.bytes";
const RETENTION_BYTES: &str = "retention.bytes";
const RETENTION_MS: &str = "retention.ms";

struct Data {
    api: Api<KafkaTopic>,
    default_partitions: u16,
    default_replication_factor: u16,
    kafka_admin_client: Arc<AdminClient<DefaultClientContext>>,
    recorder: Recorder,
}

#[derive(Error, Debug)]
enum OperatorError {
    #[error("Config error: {0}")]
    Config(#[from] ConfigError),
    #[error("Kafka error: {0}")]
    Kafka(#[from] KafkaError),
    #[error("kube API error: {0}")]
    Kube(#[from] kube::Error),
    #[error("Kafka error: {0}")]
    RDKafka(#[from] RDKafkaErrorCode),
}

fn config() -> Result<Config, ConfigError> {
    Config::builder()
        .add_source(config::File::with_name(&config_filename()))
        .build()
}

fn config_filename() -> String {
    match env::var_os(CONFIG_FILE) {
        Some(v) => v
            .to_str()
            .map_or(DEFAULT_CONFIG_FILE.to_string(), |s| s.to_string()),
        None => DEFAULT_CONFIG_FILE.to_string(),
    }
}

fn context(
    client: &Client,
    api: &Api<KafkaTopic>,
    default_partitions: u16,
    default_replication_factor: u16,
    kafka_admin_client: Arc<AdminClient<DefaultClientContext>>,
) -> Arc<Data> {
    Arc::new(Data {
        api: api.clone(),
        default_partitions,
        default_replication_factor,
        kafka_admin_client,
        recorder: Recorder::new(
            client.clone(),
            Reporter {
                controller: CONTROLLER.to_string(),
                instance: None,
            },
        ),
    })
}

async fn create_topic(topic: &KafkaTopic, ctx: &Data) -> Result<(), OperatorError> {
    let mut config = vec![];
    let max_message_bytes = topic.spec.max_message_bytes.unwrap_or(0);
    let max_message_bytes_s = max_message_bytes.to_string();
    let retention_bytes = topic.spec.retention_bytes.unwrap_or(0);
    let retention_bytes_s = retention_bytes.to_string();
    let retention_millisecond = topic.spec.retention_milliseconds.unwrap_or(0);
    let retention_millisecond_s = retention_millisecond.to_string();

    if max_message_bytes > 0 {
        config.push((MAX_MESSAGE_BYTES, max_message_bytes_s.as_str()));
    }

    if retention_bytes > 0 {
        config.push((RETENTION_BYTES, retention_bytes_s.as_str()));
    }

    if retention_millisecond > 0 {
        config.push((RETENTION_MS, retention_millisecond_s.as_str()));
    }

    ctx.kafka_admin_client
        .create_topics(
            vec![&NewTopic {
                name: &name(topic),
                num_partitions: partitions(topic, ctx),
                replication: replication(topic, ctx),
                config,
            }],
            &AdminOptions::default(),
        )
        .await?;
    Ok(())
}

fn error_policy(_obj: Arc<KafkaTopic>, _err: &OperatorError, _ctx: Arc<Data>) -> Action {
    Action::requeue(Duration::from_secs(5))
}

async fn get_topic(
    topic: &KafkaTopic,
    ctx: &Data,
) -> Result<Option<HashMap<String, String>>, OperatorError> {
    let config = ctx
        .kafka_admin_client
        .describe_configs(
            vec![&ResourceSpecifier::Topic(&name(topic))],
            &AdminOptions::default(),
        )
        .await?;

    if !config.is_empty() {
        match &config[0] {
            Ok(r) => {
                let c = topic_config(&r.entries);

                if !c.is_empty() {
                    Ok(Some(c))
                } else {
                    Ok(None)
                }
            },
            Err(e) => Err(OperatorError::from(*e)),
        }
    } else {
        Ok(None)
    }
}

fn has_changed(obj: &KafkaTopic, config: &HashMap<String, String>) -> bool {
    obj.spec.max_message_bytes.is_some_and(|desired| {
        config
            .get(MAX_MESSAGE_BYTES)
            .is_none_or(|fetched| desired.to_string() != *fetched)
    }) || obj.spec.retention_bytes.is_some_and(|desired| {
        config
            .get(RETENTION_BYTES)
            .is_none_or(|fetched| desired.to_string() != *fetched)
    }) || obj.spec.retention_milliseconds.is_some_and(|desired| {
        config
            .get(RETENTION_MS)
            .is_none_or(|fetched| desired.to_string() != *fetched)
    })
}

fn kafka_config(config: &Config) -> Result<ClientConfig, OperatorError> {
    let mut client_config = ClientConfig::new();
    let map = config.get::<HashMap<String, String>>(KAFKA)?;

    Ok(map
        .iter()
        .fold(&mut client_config, |c, (k, v)| c.set(k, v))
        .clone())
}

#[tokio::main]
async fn main() -> Result<()> {
    const VERSION: &str = "1.0.0";

    env_logger::init();
    default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let config = config()?;
    let client = Client::try_default().await?;
    let default_partitions: u16 = config.get(DEFAULT_PARTITIONS)?;
    let default_replication_factor: u16 = config.get(DEFAULT_REPLICATION_FACTOR)?;
    let kafka_admin_client: Arc<AdminClient<DefaultClientContext>> =
        Arc::new(kafka_config(&config)?.create()?);

    info!("Version: {VERSION}");

    join_all(
        watch_namespaced(client.clone())
            .iter()
            .map(|c| {
                serial_controller(c)
                    .run(
                        reconcile,
                        error_policy,
                        context(
                            &client,
                            c,
                            default_partitions,
                            default_replication_factor,
                            kafka_admin_client.clone(),
                        ),
                    )
                    .for_each(|res| async { report_reconciliation(res) })
            })
            .collect::<Vec<_>>(),
    )
    .await;

    Ok(())
}

fn name(topic: &KafkaTopic) -> String {
    topic
        .spec
        .name
        .as_ref()
        .or(topic.metadata.name.as_ref())
        .cloned()
        .unwrap_or("unknown".to_string())
}

fn next_time() -> Action {
    Action::requeue(DEFAULT_RECONCILIATION_INTERVAL)
}

fn partitions(topic: &KafkaTopic, ctx: &Data) -> i32 {
    topic.spec.partitions.unwrap_or(ctx.default_partitions) as i32
}

async fn reconcile(obj: Arc<KafkaTopic>, ctx: Arc<Data>) -> Result<Action, OperatorError> {
    if is_not_ready(obj.status()) {
        sleep(DEFAULT_BACK_OFF).await;
    }

    let topic = get_topic(&obj, &ctx).await?;
    let changed = topic.as_ref().is_some_and(|t| has_changed(&obj, t));

    if should_reconcile(obj.as_ref(), CONTROLLER)
        || topic.is_none()
        || (changed
            && has_done_update_since_at_least(obj.object_meta(), CONTROLLER, DEFAULT_BACK_OFF))
    {
        reconciliation_result(
            obj.clone(),
            &ctx,
            reconcile_action(obj.clone(), &ctx, topic.is_some()).await,
            &obj.object_ref(&()),
        )
        .await
    } else {
        Ok(next_time())
    }
}

async fn reconcile_action(
    obj: Arc<KafkaTopic>,
    ctx: &Data,
    exists: bool,
) -> Result<Action, OperatorError> {
    if exists {
        update_topic(&obj, ctx).await?;
    } else {
        create_topic(&obj, ctx).await?;
    }

    Ok(next_time())
}

async fn reconciliation_result(
    obj: Arc<KafkaTopic>,
    ctx: &Data,
    result: Result<Action, OperatorError>,
    obj_ref: &ObjectReference,
) -> Result<Action, OperatorError> {
    match result {
        Err(e) => {
            patch_status(&ctx.api, &obj, Some(&e.to_string()), CONTROLLER).await?;
            ctx.recorder
                .publish(&error_event(&e.to_string(), "update"), obj_ref)
                .await?;
            Err(e)
        }
        Ok(r) => {
            patch_status(&ctx.api, &obj, None, CONTROLLER).await?;
            Ok(r)
        }
    }
}

fn replication<'a>(topic: &KafkaTopic, ctx: &Data) -> TopicReplication<'a> {
    TopicReplication::Fixed(
        topic
            .spec
            .replication_factor
            .unwrap_or(ctx.default_replication_factor) as i32,
    )
}

fn topic_config(entries: &Vec<ConfigEntry>) -> HashMap<String, String> {
    entries
        .iter()
        .filter(|e| e.value.is_some())
        .map(|e| (e.name.clone(), e.value.as_ref().unwrap().clone()))
        .collect()
}

async fn update_topic(topic: &KafkaTopic, ctx: &Data) -> Result<(), OperatorError> {
    let mut entries = HashMap::new();
    let max_message_bytes = topic.spec.max_message_bytes.unwrap_or(0);
    let max_message_bytes_s = max_message_bytes.to_string();
    let retention_bytes = topic.spec.retention_bytes.unwrap_or(0);
    let retention_bytes_s = retention_bytes.to_string();
    let retention_millisecond = topic.spec.retention_milliseconds.unwrap_or(0);
    let retention_millisecond_s = retention_millisecond.to_string();

    if max_message_bytes > 0 {
        entries.insert("max.message.bytes", max_message_bytes_s.as_str());
    }

    if retention_bytes > 0 {
        entries.insert("retention.bytes", retention_bytes_s.as_str());
    }

    if retention_millisecond > 0 {
        entries.insert("retention.ms", retention_millisecond_s.as_str());
    }

    ctx.kafka_admin_client
        .alter_configs(
            vec![&AlterConfig {
                specifier: ResourceSpecifier::Topic(&name(topic)),
                entries,
            }],
            &AdminOptions::default(),
        )
        .await?;
    Ok(())
}
