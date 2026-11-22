use codex_otel::ManagedTelemetryRoutes;
use codex_otel::OtelExporter;
use codex_otel::OtelHttpProtocol;
use codex_otel::OtelProvider;
use codex_otel::OtelSettings;
use serde_json::Value;
use tracing_subscriber::layer::SubscriberExt;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;
use wiremock::matchers::path;

const TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const PARENT_ID: &str = "00f067aa0ba902b7";

// Exercise the real eager managed route, rather than the deferred plain OTEL
// test layer under which setting a parent after span construction still works.
async fn collect_managed_spans(make_spans: impl FnOnce()) -> Value {
    let collector = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/traces"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&collector)
        .await;
    let home = tempfile::tempdir().unwrap();
    let mut candidate = Some(
        OtelProvider::prepare(&OtelSettings {
            http_client_factory: codex_http_client::HttpClientFactory::new(
                codex_http_client::OutboundProxyPolicy::ReqwestDefault,
            ),
            environment: "test".to_owned(),
            service_name: "request-parent-test".to_owned(),
            service_version: "1".to_owned(),
            codex_home: home.path().to_path_buf(),
            exporter: OtelExporter::None,
            trace_exporter: OtelExporter::OtlpHttp {
                endpoint: format!("{}/v1/traces", collector.uri()),
                headers: Default::default(),
                protocol: OtelHttpProtocol::Json,
                tls: None,
            },
            metrics_exporter: OtelExporter::None,
            runtime_metrics: false,
            span_attributes: Default::default(),
            tracestate: Default::default(),
        })
        .unwrap(),
    );
    let (layers, routes) = ManagedTelemetryRoutes::layers();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layers));
    let mut publication = routes.publish(&mut candidate).unwrap();
    tracing::dispatcher::with_default(&dispatch, make_spans);
    let (disabled, retired) = routes
        .disable()
        .unwrap()
        .retire_previous(publication.provider.take());
    assert!(disabled.is_none());
    retired
        .wait_until(tokio::time::Instant::now() + std::time::Duration::from_secs(8))
        .await
        .unwrap();
    let requests = collector.received_requests().await.unwrap();
    assert!(
        !requests.is_empty(),
        "managed exporter must actually send spans"
    );
    Value::Array(
        requests
            .into_iter()
            .map(|request| serde_json::from_slice(&request.body).unwrap())
            .collect(),
    )
}

fn find_span<'a>(batches: &'a Value, name: &str) -> &'a Value {
    batches
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|batch| batch["resourceSpans"].as_array().unwrap())
        .flat_map(|resource| resource["scopeSpans"].as_array().unwrap())
        .flat_map(|scope| scope["spans"].as_array().unwrap())
        .find(|span| span["name"] == name)
        .unwrap_or_else(|| panic!("missing managed span {name}: {batches}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial_test::serial(app_server_tracing)]
async fn incoming_request_parent_is_selected_before_managed_eager_creation() {
    let spans = collect_managed_spans(|| {
        let ambient = tracing::info_span!("unrelated_ambient");
        let _entered = ambient.enter();
        let request = codex_app_server_protocol::JSONRPCRequest {
            id: codex_app_server_protocol::RequestId::Integer(17),
            method: "thread/start".to_owned(),
            params: None,
            trace: Some(codex_protocol::protocol::W3cTraceContext {
                traceparent: Some(format!("00-{TRACE_ID}-{PARENT_ID}-01")),
                tracestate: None,
            }),
        };
        let session = crate::message_processor::ConnectionSessionState::new(
            crate::transport::ConnectionOrigin::Stdio,
        );
        drop(super::request_span(
            &request,
            &crate::transport::AppServerTransport::Stdio,
            crate::outgoing_message::ConnectionId(7),
            &session,
        ));
    })
    .await;
    // OTEL exports otel.name (the RPC method), not the tracing callsite name.
    let request = find_span(&spans, "thread/start");
    assert_eq!(request["kind"], 2);
    assert!(
        request["attributes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|attribute| {
                attribute["key"] == "rpc.method"
                    && attribute["value"]["stringValue"] == "thread/start"
            })
    );
    assert_eq!(request["traceId"], TRACE_ID);
    assert_eq!(request["parentSpanId"], PARENT_ID);
    assert_ne!(find_span(&spans, "unrelated_ambient")["traceId"], TRACE_ID);
}
