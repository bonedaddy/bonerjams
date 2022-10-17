use anyhow::{anyhow, Result};
use bonerjams_config::Configuration;
use clap::{App, Arg, SubCommand};

#[tokio::main]
async fn main() -> Result<()> {
    let key_flag = Arg::with_name("key")
        .long("key")
        .takes_value(true)
        .required(true);
    let value_flag = Arg::with_name("value")
        .long("value")
        .takes_value(true)
        .required(true);

    let matches = App::new("mevdaddy")
        .version("0.0.1")
        .author("solfarm")
        .about("template cli for rust projects")
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .value_name("FILE")
                .help("sets the config file")
                .takes_value(true),
        )
        .subcommand(
            SubCommand::with_name("config")
                .about("config management commands")
                .subcommands(vec![
                    SubCommand::with_name("new").arg(
                        Arg::with_name("config")
                            .short("c")
                            .long("config")
                            .value_name("FILE")
                            .help("sets the config file")
                            .takes_value(true),
                    ),
                    SubCommand::with_name("new-certificate").arg(
                        Arg::with_name("hosts")
                            .long("hosts")
                            .help("the hosts to create the certificate for")
                            .min_values(1)
                            .required(true),
                    ),
                ]),
        )
        .subcommand(SubCommand::with_name("server").about("run the bonerjams kv server"))
        .subcommand(
            SubCommand::with_name("client")
                .about("bonerjams client management")
                .subcommands(vec![
                    SubCommand::with_name("put")
                        .arg(key_flag.clone())
                        .arg(value_flag.clone()),
                    SubCommand::with_name("get")
                        .arg(key_flag.clone())
                        .arg(
                            Arg::with_name("validitiy-period")
                                .long("validity-period")
                                .help("the number of days  the  cert is validate for")
                                .takes_value(true)
                                .required(true),
                        )
                        .arg(
                            Arg::with_name("is-ca")
                                .long("is-ca")
                                .help("if present, marks the certificate as a ca certificate")
                                .takes_value(false)
                                .required(false),
                        )
                        .arg(
                            Arg::with_name("rsa")
                                .long("rsa")
                                .help("if present generate an rsa certificate, otherwise secp256r1")
                                .takes_value(false)
                                .required(false),
                        ),
                ]),
        )
        .get_matches();
    let config_file_path = get_config_or_default(&matches);
    process_matches(&matches, &config_file_path).await?;
    Ok(())
}
async fn process_matches<'a>(matches: &clap::ArgMatches<'a>, config_file_path: &str) -> Result<()> {
    match matches.subcommand() {
        ("config", Some(conf_cmd)) => match conf_cmd.subcommand() {
            ("new", Some(_)) => Ok(Configuration::default().save(config_file_path, false)?),
            ("new-grpc-certificate", Some(new_cert)) => {
                let hosts = new_cert
                    .values_of("hosts")
                    .unwrap()
                    .collect::<Vec<_>>()
                    .iter()
                    .map(|host| host.to_string())
                    .collect::<Vec<_>>();

                let cert = bonerjams_db::rpc::self_signed_cert::SelfSignedCert::new(
                    &hosts[..],
                    matches
                        .value_of("validity-period-days")
                        .unwrap()
                        .parse()
                        .unwrap(),
                    matches.is_present("rsa"),
                    matches.is_present("is-ca"),
                )?;
                println!("base64 key {}", cert.base64_key);
                println!("base64 cert {}", cert.base64_cert);
                Ok(())
            }
            _ => invalid_subcommand("config"),
        },
        ("server", Some(_)) => {
            let conf = get_config(config_file_path)?;
            bonerjams_config::init_log(false)?;
            bonerjams_db::rpc::start_server(conf).await
        }
        ("client", Some(client_cmd)) => match client_cmd.subcommand() {
            ("put", Some(put_cmd)) => {
                let conf = get_config(config_file_path)?;
                bonerjams_config::init_log(false)?;
                let client = bonerjams_db::rpc::client::Client::new(
                    &conf,
                    &conf.rpc.auth_token,
                    !conf.rpc.tls_cert.is_empty() && !conf.rpc.tls_key.is_empty(),
                )
                .await?;
                Ok(client
                    .put(
                        put_cmd.value_of("key").unwrap().as_bytes(),
                        put_cmd.value_of("value").unwrap().as_bytes(),
                    )
                    .await?)
            }
            ("get", Some(get_cmd)) => {
                let conf = get_config(config_file_path)?;
                bonerjams_config::init_log(false)?;
                let client = bonerjams_db::rpc::client::Client::new(
                    &conf,
                    &conf.rpc.auth_token,
                    !conf.rpc.tls_cert.is_empty() && !conf.rpc.tls_key.is_empty(),
                )
                .await?;
                let val = client
                    .get(get_cmd.value_of("key").unwrap().as_bytes())
                    .await?;
                log::info!("val {}", String::from_utf8(val)?);
                Ok(())
            }
            _ => invalid_subcommand("client"),
        },
        _ => invalid_command(),
    }
}
// returns the value of the config file argument or the default
fn get_config_or_default(matches: &clap::ArgMatches) -> String {
    matches
        .value_of("config")
        .unwrap_or("config.yaml")
        .to_string()
}

pub fn get_config(path: &str) -> Result<Configuration> {
    Configuration::load(path, false)
}

fn invalid_subcommand(command_group: &str) -> Result<()> {
    Err(anyhow!("invalid command found for group {}", command_group))
}

fn invalid_command() -> Result<()> {
    Err(anyhow!("invalid command found"))
}
