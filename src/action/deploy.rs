use std::error::Error;

pub(super) async fn run() -> Result<(), Box<dyn Error + Send + Sync>> {
    println!("je lance le traitement de type 'deploy'");
    Ok(())
}