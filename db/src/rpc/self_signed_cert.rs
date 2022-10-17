//! wrapper around multiple libraries to generate self signed certificates

use anyhow::{anyhow, Result};
use chrono::prelude::*;
use config::RPC;
use openssl::{
    ec::{EcGroup, EcKey},
    nid::Nid, x509::{X509NameRef, X509Name},
};
use rcgen::{date_time_ymd, IsCa, BasicConstraints, PublicKey, KeyIdMethod};
use rcgen::{
    generate_simple_self_signed, Certificate, CertificateParams, DistinguishedName, SanType,
};
use ring::signature::{EcdsaKeyPair, EcdsaSigningAlgorithm};
use tonic::IntoRequest;

/// provides a self-signed certificate 
#[derive(Clone, Debug)]
pub struct SelfSignedCert {
    /// base64 pem encoded keypair
    pub base64_key: String,
    /// base64 pem encoded certificate
    pub base64_cert: String,
    /// indicates if the certificate is a ca certificate
    /// `None` means this information is unknown
    pub is_ca: Option<bool>,
}

impl From<&RPC> for SelfSignedCert {
    fn from(rpc: &RPC) -> Self {
        Self {
            base64_cert: rpc.tls_cert.clone(),
            base64_key: rpc.tls_key.clone(),
            is_ca: None,
        }
    }
}


impl From<(&str, &str)> for SelfSignedCert {
    fn from(inputs: (&str, &str)) -> Self {
        Self {
            base64_cert: inputs.1.to_string(),
            base64_key: inputs.0.to_string(),
            is_ca: None,
        }
    }
}
impl SelfSignedCert {
    pub fn new(
        subject_alt_names: &[String],
        validity_period_days: u64,
        rsa: bool,
        ca: bool
    ) -> Result<Self> {
        let now = Utc::now();
        let then = now
            .checked_add_signed(chrono::Duration::days(validity_period_days as i64))
            .unwrap();
        let mut params: CertificateParams = CertificateParams::new(subject_alt_names);
        if ca {
            params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        }
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
        
        let crt = base64::encode(pem_cert_serialized);
        let key = base64::encode(pem_key_serialized);
    
        Ok(SelfSignedCert {
            base64_cert: crt,
            base64_key: key,
            is_ca: Some(ca),
        })
    }
    /// base64 decodes the certificate key, and returns the PEM formatted key
    pub fn key(&self) -> Result<String> {
        Ok(String::from_utf8(base64::decode(&self.base64_key)?)?)
    }
    /// base64 decodes the certificate and returns the PEM Formatted cert
    pub fn cert(&self) -> Result<String> {
        Ok(String::from_utf8(base64::decode(&self.base64_cert)?)?)
    }
}

#[cfg(test)]
mod test {
    use super::*;
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
}
