use anyhow::{anyhow, Result};
use chrono::prelude::*;
use config::RPC;
use openssl::{
    ec::{EcGroup, EcKey},
    nid::Nid, x509::{X509NameRef, X509Name},
};
use rcgen::{date_time_ymd, IsCa, BasicConstraints, PublicKey};
use rcgen::{
    generate_simple_self_signed, Certificate, CertificateParams, DistinguishedName, SanType,
};
use ring::signature::{EcdsaKeyPair, EcdsaSigningAlgorithm};
use tonic::IntoRequest;

#[derive(Clone, Debug)]
pub struct SelfSignedCert {
    /// base64 pem encoded keypair
    pub base64_key: String,
    /// base64 pem encoded certificate
    pub base64_cert: String,
}

impl From<&RPC> for SelfSignedCert {
    fn from(rpc: &RPC) -> Self {
        Self {
            base64_cert: rpc.tls_cert.clone(),
            base64_key: rpc.tls_key.clone()
        }
    }
}

impl SelfSignedCert {
    pub fn new(key: &str, cert: &str) -> Self {
        Self {
            base64_cert: cert.to_string(),
            base64_key: key.to_string(),
        }
    }
    pub fn key(&self) -> Result<String> {
        Ok(String::from_utf8(base64::decode(&self.base64_key)?)?)
    }
    pub fn cert(&self) -> Result<String> {
        Ok(String::from_utf8(base64::decode(&self.base64_cert)?)?)
    }
}

/// Generates a self-signed PEM encoded keypair/certificate pair.
/// If `rsa` is true, a 4096 bit SHA256 RSA keypair is generate
/// otherwise a SECP384R1 keypair is generated
pub fn create_self_signed(
    subject_alt_names: &[String],
    validity_period_days: u64,
    rsa: bool,
) -> Result<SelfSignedCert> {
    let now = Utc::now();
    let then = now
        .checked_add_signed(chrono::Duration::days(validity_period_days as i64))
        .unwrap();
    let mut params: CertificateParams = CertificateParams::new(subject_alt_names);
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.not_before = date_time_ymd(now.year().into(), now.month() as u8, now.day() as u8);
    params.not_after = date_time_ymd(then.year().into(), then.month() as u8, then.day() as u8);

    if rsa {
        params.alg = &rcgen::PKCS_RSA_SHA256;

        params.key_pair = {
            let pkey: openssl::pkey::PKey<_> = openssl::rsa::Rsa::generate(4096)?.try_into()?;
            let pkey_pem = String::from_utf8(pkey.private_key_to_pem_pkcs8()?)?;
            Some(rcgen::KeyPair::from_pem(&pkey_pem)?)
        };
    } else {
        params.alg = &rcgen::PKCS_ECDSA_P256_SHA256;
        params.key_pair = { Some(rcgen::KeyPair::generate(&rcgen::PKCS_ECDSA_P256_SHA256)?) };
    }
    let cert = Certificate::from_params(params)?;
    
    let pem_cert_serialized = cert.serialize_pem()?;
    let pem_key_serialized = cert.serialize_private_key_pem();
    let pem_cert_der_serialized = pem::parse(&pem_cert_serialized).unwrap().contents;

    let hash = ring::digest::digest(&ring::digest::SHA512, &pem_cert_der_serialized);
    let hash_hex: String = hash.as_ref().iter().map(|b| format!("{b:02x}")).collect();

    println!("hash hex {}", hash_hex);

    let crt = base64::encode(pem_cert_serialized);
    let key = base64::encode(pem_key_serialized);

    Ok(SelfSignedCert {
        base64_cert: crt,
        base64_key: key,
    })
}


/// Generates a self-signed PEM encoded keypair/certificate pair.
/// If `rsa` is true, a 4096 bit SHA256 RSA keypair is generate
/// otherwise a SECP384R1 keypair is generated
pub fn create_self_signed2(
    subject_alt_names: &[String],
) -> Result<SelfSignedCert> {
    let cert = generate_simple_self_signed(subject_alt_names)?;
    
    let pem_cert_serialized = cert.serialize_pem()?;
    let pem_key_serialized = cert.serialize_private_key_pem();
    let pem_cert_der_serialized = pem::parse(&pem_cert_serialized).unwrap().contents;

    let hash = ring::digest::digest(&ring::digest::SHA512, &pem_cert_der_serialized);
    let hash_hex: String = hash.as_ref().iter().map(|b| format!("{b:02x}")).collect();

    println!("hash hex {}", hash_hex);

    let crt = base64::encode(pem_cert_serialized);
    let key = base64::encode(pem_key_serialized);

    Ok(SelfSignedCert {
        base64_cert: crt,
        base64_key: key,
    })
}

/*
// (base: https://github.com/ctz/hyper-rustls/blob/5f073724f7b5eee3a2d72f0a86094fc2718b51cd/examples/server.rs)
pub fn load_tls_config(
    cert_path: impl AsRef<std::path::Path>,
    key_path: impl AsRef<std::path::Path> + std::fmt::Display,
) -> std::io::Result<rustls::ServerConfig> {
    // Load public certificate.
    let mut cert_reader = std::io::BufReader::new(std::fs::File::open(cert_path)?);
    let certs = rustls::internal::pemfile::certs(&mut cert_reader)
        .map_err(|_| error("unable to load certificate".to_owned()))?;
    // Load private key.
    let mut key_reader = std::io::BufReader::new(std::fs::File::open(key_path)?);
    // Load and return a single private key.
    let key = rustls::internal::pemfile::pkcs8_private_keys(&mut key_reader)
        .map_err(|_| error("unable to load private key".to_owned()))?
        .remove(0);
    // Do not use client certificate authentication.
    let mut cfg = rustls::ServerConfig::new(rustls::NoClientAuth::new());
    // Select a certificate to use.
    cfg.set_single_cert(certs, key).unwrap();
    // Configure ALPN to accept HTTP/2, HTTP/1.1 in that order.
    cfg.set_protocols(&[b"h2".to_vec(), b"http/1.1".to_vec()]);
    Ok(cfg)
}*/
#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_create_rsa_self_signed() {
        let cert = create_self_signed(&["escort".to_string()], 1000, true).unwrap();
        println!("key {}", cert.key().unwrap());
        println!("cert {}", cert.cert().unwrap());
    }
    #[test]
    fn test_create_self_signed() {
        let cert = create_self_signed(&["escort".to_string()], 1000, false).unwrap();
        println!("key {}", cert.key().unwrap());
        println!("cert {}", cert.cert().unwrap());
    }
}