use std::error::Error;

use serde::Deserialize;

mod deploy;
mod undeploy;

type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Deploy,
    Undeploy,
}

pub async fn dispatch(action: Action) -> Result<(), BoxError> {
    match action {
        Action::Deploy => deploy::run().await,
        Action::Undeploy => undeploy::run().await,
    }
}