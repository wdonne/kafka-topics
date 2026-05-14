mod resource;

use kube::CustomResourceExt;
use resource::KafkaTopic;

fn main() {
    print!(
        "{}",
        serde_yaml::to_string(&KafkaTopic::crd()).unwrap()
    )
}
