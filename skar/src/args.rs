use clap::Parser;

#[derive(Parser, Debug, Clone)]
pub struct Args {
    #[clap(long, default_value_t = default_config_path())]
    pub config_path: String,
}

fn default_config_path() -> String {
    "skar.toml".to_owned()
}

impl Args {
    pub fn parse() -> Self {
        <Self as Parser>::parse()
    }
}
