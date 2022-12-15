use axum::response::Response;
use axum::{http::StatusCode, response::IntoResponse, Json};
use serde_json::{json, Value};
use thiserror::Error;
#[derive(Debug, Error, Clone)]
pub enum Error {
    #[error("{0}")]
    CustomError(String),
    #[error("{0}")]
    CustomServerError(String),
}
pub type Result<T> = std::result::Result<T, Error>;

pub type ApiError = (StatusCode, Json<Value>);
pub type ApiResult<T> = std::result::Result<T, ApiError>;

impl From<Error> for ApiError {
    fn from(err: Error) -> Self {
        let (status, msg) = match err {
            Error::CustomError(err_msg) => (StatusCode::BAD_REQUEST, err_msg),
            Error::CustomServerError(err_msg) => (StatusCode::BAD_REQUEST, err_msg),
        };
        let payload = json!({ "message": msg });
        (status, Json(payload))
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, msg) = match self {
            Error::CustomError(err_msg) => (StatusCode::BAD_REQUEST, err_msg),
            Error::CustomServerError(err_msg) => (StatusCode::BAD_REQUEST, err_msg),
        };

        let body = Json(json!({
            "error": msg,
        }));

        (status, body).into_response()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_custom_error() {
        println!(
            "{:#?}",
            ApiError::from(Error::CustomError(
                "this is a test custom error message".to_string()
            ))
        );
    }
}
