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
async fn request_and_detached_process_keep_managed_eager_parent_context() {
    let spans = collect_managed_spans(|| {
        let message = codex_exec_server_protocol::JSONRPCMessage::Request(
            codex_exec_server_protocol::JSONRPCRequest {
                id: codex_exec_server_protocol::RequestId::Integer(17),
                method: "process/start".to_owned(),
                params: None,
                trace: Some(codex_protocol::protocol::W3cTraceContext {
                    traceparent: Some(format!("00-{TRACE_ID}-{PARENT_ID}-01")),
                    tracestate: None,
                }),
            },
        );
        let crate::connection::JsonRpcConnectionEvent::QueuedRequest { request_span, .. } =
            crate::connection::JsonRpcConnectionEvent::message(message)
        else {
            panic!("request must be queued with its span");
        };
        let process = {
            let _entered = request_span.enter();
            crate::telemetry::ExecServerTelemetry::default().process_started("test-process")
        };
        // Process custody must not retain the request's tracing span as a local parent.
        drop(request_span);
        drop(process);
    })
    .await;
    // The queued request has the explicit OTEL name "unknown" until dispatch.
    let request = find_span(&spans, "unknown");
    assert_eq!(request["kind"], 2);
    assert!(
        request["attributes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|attribute| {
                attribute["key"] == "method" && attribute["value"]["stringValue"] == "process/start"
            })
    );
    let process = find_span(&spans, "codex.exec_server.process");
    assert_eq!(request["traceId"], TRACE_ID);
    assert_eq!(request["parentSpanId"], PARENT_ID);
    assert_eq!(process["traceId"], TRACE_ID);
    assert_eq!(process["parentSpanId"], request["spanId"]);
}
