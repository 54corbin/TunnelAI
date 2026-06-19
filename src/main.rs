use anyhow::Result;
use clap::Parser;
use tunnel_ai::cli::{Cli, Command};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Server(args) => tunnel_ai::server::run(args).await,
        Command::Client(args) => tunnel_ai::client::run(args).await,
        Command::OpenAiServer(args) => tunnel_ai::openai::run_server(args).await,
        Command::OpenAiClient(args) => tunnel_ai::openai::run_client(args).await,
    }
}
