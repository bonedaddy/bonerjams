use bonerjams_config::{database::DbOpts, Configuration, ConnType, RpcHost, RpcPort, RPC};
use bonerjams_db::prelude::*;
use std::collections::HashMap;

#[tokio::test(flavor = "multi_thread")]
#[allow(unused_must_use)]
async fn test_run_server_tcp() {
    let conf = Configuration {
        db: DbOpts {
            path: "/tmp/kek2232222.db".to_string(),
            ..Default::default()
        },
        rpc: RPC {
            auth_token: "Bearer some-secret-tokennnnnnn".to_string(),
            connection: ConnType::HTTP(
                "127.0.0.1".to_string() as RpcHost,
                "8668".to_string() as RpcPort,
            ),
            ..Default::default()
        },
    };
    bonerjams_config::init_log(true);

    run_server(conf, false,  false).await;
}

#[tokio::test(flavor = "multi_thread")]
#[allow(unused_must_use)]
async fn test_run_server_tcp_no_token_auth() {
    let conf = Configuration {
        db: DbOpts {
            path: "/tmp/kek2232222.db".to_string(),
            ..Default::default()
        },
        rpc: RPC {
            auth_token: "".to_string(),
            connection: ConnType::HTTP(
                "127.0.0.1".to_string() as RpcHost,
                "8668".to_string() as RpcPort,
            ),
            ..Default::default()
        },
    };
    bonerjams_config::init_log(true);

    run_server(conf, false,  false).await;
}
#[tokio::test(flavor = "multi_thread")]
#[allow(unused_must_use)]
async fn test_run_server_tcp_tls() {
    // https://github.com/LucioFranco/tonic-openssl/blob/master/example/src/server.rs
    let self_signed = SelfSignedCert::new(
        &[
            "https://localhost:8668".to_string(),
            "localhost".to_string(),
            "localhost:8668".to_string(),
        ],
        1000,
        false,
        false,
    )
    .unwrap();
    let conf = Configuration {
        db: DbOpts {
            path: "/tmp/kek2232222.db".to_string(),
            ..Default::default()
        },
        rpc: RPC {
            auth_token: "Bearer some-secret-tokennnnnnn".to_string(),
            connection: ConnType::HTTPS(
                "localhost".to_string() as RpcHost,
                "8668".to_string() as RpcPort,
            ),
            tls_cert: self_signed.base64_cert,
            tls_key: self_signed.base64_key,
        },
    };
    bonerjams_config::init_log(true);

    run_server(conf, true,  false).await;
}

#[tokio::test(flavor = "multi_thread")]
#[allow(unused_must_use)]
async fn test_run_server_tcp_tls_no_token_auth() {
    // https://github.com/LucioFranco/tonic-openssl/blob/master/example/src/server.rs
    let self_signed = SelfSignedCert::new(
        &[
            "https://localhost:8668".to_string(),
            "localhost".to_string(),
            "localhost:8668".to_string(),
        ],
        1000,
        false,
        false,
    )
    .unwrap();
    let conf = Configuration {
        db: DbOpts {
            path: "/tmp/kek2232222.db".to_string(),
            ..Default::default()
        },
        rpc: RPC {
            auth_token: "".to_string(),
            connection: ConnType::HTTPS(
                "localhost".to_string() as RpcHost,
                "8668".to_string() as RpcPort,
            ),
            tls_cert: self_signed.base64_cert,
            tls_key: self_signed.base64_key,
        },
    };
    bonerjams_config::init_log(true);

    run_server(conf, true, false).await;
}
async fn run_server(conf: bonerjams_config::Configuration, tls: bool, pubsub: bool) {
    {
        let conf = conf.clone();
        tokio::spawn(async move { bonerjams_db::rpc::start_server(conf, pubsub).await });
    }
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let client = Client::new(&conf, "Bearer some-secret-tokennnnnnn", tls)
        .await
        .unwrap();
    client.ready().await.unwrap();
    client
        .put(
            "four twenty blaze".as_bytes(),
            "sixty nine gigity".as_bytes(),
        )
        .await
        .unwrap();
    client.put("1".as_bytes(), "2".as_bytes()).await.unwrap();
    client.put("3".as_bytes(), "4".as_bytes()).await.unwrap();
    let response = client.get("four twenty blaze".as_bytes()).await.unwrap();
    println!("key 'four twenty blaze', value {:?}", unsafe {
        String::from_utf8_unchecked(response)
    });

    let mut entries = HashMap::new();
    entries.insert(
        "".to_string(),
        vec![KeyValue {
            key: "sixety_nine".as_bytes().to_vec(),
            value: "l33tm0d3".as_bytes().to_vec(),
        }],
    );
    entries.insert(
        base64::encode(vec![4, 2, 0]),
        vec![KeyValue {
            key: "sixety_nine".as_bytes().to_vec(),
            value: "l33tm0d3".as_bytes().to_vec(),
        }],
    );
    client
        .batch_put(vec![
            (
                vec![],
                vec![BatchPutEntry {
                    key: "sixety_nine".as_bytes().to_vec(),
                    value: "l33tm0d3".as_bytes().to_vec(),
                }],
            ),
            (
                vec![4, 2, 0],
                vec![BatchPutEntry {
                    key: "sixety_nine".as_bytes().to_vec(),
                    value: "l33tm0d3".as_bytes().to_vec(),
                }],
            ),
        ])
        .await
        .unwrap();
    let response = client.get("four twenty blaze".as_bytes()).await.unwrap();
    println!("response {}", unsafe {
        String::from_utf8_unchecked(response)
    });
    client
        .list(&[])
        .await
        .unwrap()
        .iter()
        .for_each(|key_value| {
            println!(
                "key {}, value {}",
                unsafe { String::from_utf8_unchecked(key_value.key.clone()) },
                unsafe { String::from_utf8_unchecked(key_value.value.clone()) }
            )
        });
    client
        .batch_exists(vec![
            (vec![], vec!["sixety_nine".as_bytes().to_vec()]),
            (
                vec![4, 2, 0],
                vec![
                    "sixety_nine".as_bytes().to_vec(),
                    "foobarbaz".as_bytes().to_vec(),
                ],
            ),
        ])
        .await
        .unwrap()
        .entries
        .iter()
        .for_each(|exists_tree| println!("{:#?}", exists_tree));
}
