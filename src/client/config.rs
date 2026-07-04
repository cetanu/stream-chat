use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct ClientConfig {
    #[arg(short = 'n', long, default_value_t = 10)]
    pub limit: usize,

    #[arg(short, long, default_value = "http://[::1]:50051")]
    pub address: String,
}

impl ClientConfig {
    pub fn parse_args() -> Self {
        Self::parse()
    }
}
