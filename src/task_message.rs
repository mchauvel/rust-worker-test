use serde::Deserialize;

use crate::action::{Action, DeployPayload, UndeployPayload};

#[derive(Deserialize)]
struct TaskMessage {
    action: String,
    #[serde(default)]
    release_name: Option<String>,
    #[serde(default)]
    chart: Option<String>,
    #[serde(default)]
    namespace: Option<String>,
    #[serde(default)]
    values: Vec<String>,
    #[serde(default)]
    set: Vec<String>,
}

pub(crate) fn parse_action(payload: &[u8]) -> Result<Action, String> {
    let message: TaskMessage =
        serde_json::from_slice(payload).map_err(|error| error.to_string())?;

    match message.action.as_str() {
        "deploy" => {
            let release_name = message
                .release_name
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| "deploy.release_name is required".to_string())?;
            let chart = message
                .chart
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| "deploy.chart is required".to_string())?;
            let namespace = message
                .namespace
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "default".to_string());
            Ok(Action::Deploy(DeployPayload {
                release_name,
                chart,
                namespace,
                values: message.values,
                set: message.set,
            }))
        }
        "undeploy" => {
            let release_name = message
                .release_name
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| "undeploy.release_name is required".to_string())?;
            let namespace = message
                .namespace
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "default".to_string());
            Ok(Action::Undeploy(UndeployPayload {
                release_name,
                namespace,
            }))
        }
        action => Err(format!("unknown action '{action}'")),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_action;
    use crate::action::Action;

    #[test]
    fn parses_deploy_action_from_json_message() {
        let action = parse_action(
            br#"{"action":"deploy","release_name":"demo","chart":"./chart","namespace":"prod"}"#,
        )
        .unwrap();
        assert!(matches!(action, Action::Deploy(_)));
    }

    #[test]
    fn rejects_unknown_action() {
        assert!(parse_action(br#"{"action":"unknown"}"#).is_err());
    }

    #[test]
    fn parses_undeploy_action_from_json_message() {
        let action =
            parse_action(br#"{"action":"undeploy","release_name":"demo","namespace":"prod"}"#)
                .unwrap();
        assert!(matches!(action, Action::Undeploy(_)));
    }
}
