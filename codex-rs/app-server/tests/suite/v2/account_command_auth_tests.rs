//! Actual RPC controls for the versioned synthetic command-auth owner fixture.
use super::*;
use base64::Engine;
use sha2::Digest;
use std::os::unix::fs::PermissionsExt;

async fn command_auth_server(home: &Path) -> Result<(TestAppServer, Vec<u8>)> {
    create_config_toml(
        home,
        CreateConfigTomlParams {
            requires_openai_auth: Some(true),
            chatgpt_base_url: Some("http://127.0.0.1:0".to_string()),
            ..Default::default()
        },
    )?;
    let expires_at = Utc::now().timestamp() + 3600;
    let claims = json!({"exp": expires_at, "https://api.openai.com/auth": {
        "chatgpt_account_id": "account", "chatgpt_user_id": "principal"
    }});
    let token = format!(
        "e30.{}.signature",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(claims.to_string())
    );
    let envelope = json!({"version": 1, "source": "source", "binding": "fixture#example",
        "account": "account", "principal": "principal", "generation": 1,
        "accessToken": token, "expiresAt": expires_at,
        "fingerprint": format!("{:x}", sha2::Sha256::digest(token.as_bytes()))});
    let bytes = serde_json::to_vec(&json!({"auth_mode": "chatgptAuthTokens",
        "tokens": {"id_token": token, "access_token": token, "refresh_token": "",
            "account_id": "account"}, "external_auth_owner": envelope}))?;
    let auth_path = home.join("auth.json");
    std::fs::write(&auth_path, &bytes)?;
    std::fs::set_permissions(&auth_path, std::fs::Permissions::from_mode(0o400))?;
    let server = TestAppServer::builder()
        .with_codex_home(home)
        .without_auto_env()
        .without_managed_config()
        .with_args(&[
            "-c",
            "cli_auth_credentials_store=\"file\"",
            "-c",
            "features.plugins=false",
        ])
        .with_env_overrides(&[
            (
                "CODEX_CHATGPT_AUTH_COMMAND",
                Some("/nonexistent/synthetic-provider"),
            ),
            ("CODEX_CHATGPT_AUTH_BINDING", Some("fixture#example")),
            ("CODEX_CHATGPT_AUTH_SOURCE", Some("source")),
            ("CODEX_CHATGPT_AUTH_ACCOUNT", Some("account")),
            ("CODEX_CHATGPT_AUTH_PRINCIPAL", Some("principal")),
            ("OPENAI_FEDERATION_RULE_ID", None),
            ("OPENAI_IDENTITY_TOKEN_FILE", None),
            ("CODEX_ACCESS_TOKEN", None),
            ("OPENAI_API_KEY", None),
        ])
        .build_initialized_with_timeout(DEFAULT_READ_TIMEOUT)
        .await?;
    Ok((server, bytes))
}

#[tokio::test]
async fn command_auth_status_with_token_does_not_export_host_owned_token() -> Result<()> {
    let home = TempDir::new()?;
    let (mut server, before) = command_auth_server(home.path()).await?;
    for refresh in [false, true] {
        let id = server
            .send_get_auth_status_request(GetAuthStatusParams {
                include_token: Some(true),
                refresh_token: Some(refresh),
            })
            .await?;
        let response: GetAuthStatusResponse =
            timeout(DEFAULT_READ_TIMEOUT, server.read_response(id)).await??;
        assert!(
            response.auth_method.is_some(),
            "fixture must have selected command auth"
        );
        assert_eq!(response.auth_token, None);
        assert_eq!(response.requires_openai_auth, Some(true));
        assert_eq!(std::fs::read(home.path().join("auth.json"))?, before);
    }
    Ok(())
}

#[tokio::test]
async fn command_auth_account_login_requests_are_host_owned() -> Result<()> {
    let home = TempDir::new()?;
    let (mut server, before) = command_auth_server(home.path()).await?;
    for params in [
        json!({"type": "apiKey", "apiKey": "synthetic-replacement"}),
        json!({"type": "chatgpt"}),
        json!({"type": "chatgptDeviceCode"}),
        json!({"type": "amazonBedrock", "apiKey": "synthetic", "region": "us-west-2"}),
        json!({"type": "chatgptAuthTokens", "accessToken": "synthetic",
            "chatgptAccountId": "other"}),
    ] {
        let id = server.send_login_account_request(params).await?;
        let error = timeout(
            DEFAULT_READ_TIMEOUT,
            server.read_stream_until_error_message(RequestId::Integer(id)),
        )
        .await??;
        assert_eq!(error.error.code, -32600);
        assert!(error.error.message.contains("owned by the app-server host"));
        assert_eq!(std::fs::read(home.path().join("auth.json"))?, before);
    }
    let id = server.send_logout_account_request().await?;
    let error = timeout(
        DEFAULT_READ_TIMEOUT,
        server.read_stream_until_error_message(RequestId::Integer(id)),
    )
    .await??;
    assert_eq!(error.error.code, -32600);
    assert!(error.error.message.contains("owned by the app-server host"));
    assert_eq!(std::fs::read(home.path().join("auth.json"))?, before);
    Ok(())
}
