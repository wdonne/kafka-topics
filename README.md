# The Kafka Topics Operator

With this Kubernetes operator you can manage Kafka topics. The `KafkaTopic` custom resource describes a Kafka topic. It will create the topic if it doesn't exist and make sure the properties it supports are not changed in any other way. When it detects such a change it will put back the values in the custom resource. When such a resource is deleted, the Kafka topic will not be deleted. A resource looks like this:

```yaml
apiVersion: pincette.net/v1
kind: KafkaTopic
metadata:
  name: test-topic
  namespace: test-kafka-topics
spec:
  maxMessageBytes: 1000000
  retentionBytes: -1
  retentionMilliseconds: 604800000
  partitions: 1
  replicationFactor: 1
```

The field `name` is optional. You can use it when the topic name doesn't comply with the 
restrictions for `metadata.name`. Defaults are defined at the level of the Kafka cluster and 
with the configuration fields `default-partitions` and `default-replication-factor`.

Install the operator as follows:

```bash
helm repo add wdonne https://wdonne.github.io/helm
helm repo update
helm install kafka-topics wdonne/kafka-topics --namespace kafka-topics --create-namespace
```

The default chart values expect you to provide a `ConfigMap` in the `kafka-topics` namespace (or 
the one you have chosen) with the name `config` like this:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  namespace: kafka-topics
  name: config
data:
  application.yaml: |
    default-partitions: 1
    default-replication-factor: 3
    kafka:
      bootstrap.servers: "localhost:9092"
```

The format of the configuration file can be anything described in [Rust Config](https://docs.rs/config/latest/config/). If your configuration has partly secret information and partly non-secret information, then you can load both a secret and a config map. Then you can include one in the other. The default command in the container image expects to find the configuration as `/conf/application`, but you can change this in the values file.
