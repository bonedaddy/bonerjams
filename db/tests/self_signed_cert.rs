use bonerjams_db::prelude::*;

#[test]
fn test_create_self_signed_rsa4096() {
    let cert = SelfSignedCert::new(&["escort".to_string()], 1000, true, false).unwrap();
    println!("key {}", cert.key().unwrap());
    println!("cert {}", cert.cert().unwrap());
}
#[test]
fn test_create_self_signed_rsa4096_ca() {
    let cert = SelfSignedCert::new(&["escort".to_string()], 1000, true, true).unwrap();
    println!("key {}", cert.key().unwrap());
    println!("cert {}", cert.cert().unwrap());
}
#[test]
fn test_create_self_signed_secp256r1() {
    let cert = SelfSignedCert::new(&["escort".to_string()], 1000, false, false).unwrap();
    println!("key {}", cert.key().unwrap());
    println!("cert {}", cert.cert().unwrap());
}
#[test]
fn test_create_self_signed_secp256r1_ca() {
    let cert = SelfSignedCert::new(&["escort".to_string()], 1000, false, true).unwrap();
    println!("key {}", cert.key().unwrap());
    println!("cert {}", cert.cert().unwrap());
}