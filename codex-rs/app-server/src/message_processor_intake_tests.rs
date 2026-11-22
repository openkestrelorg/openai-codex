use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

#[tokio::test]
async fn real_processor_intake_closes_admission_and_latches_existing_cleanup_owners() {
    let home = TempDir::new().unwrap();
    let config = Arc::new(
        codex_core::config::ConfigBuilder::default()
            .codex_home(home.path().to_path_buf())
            .cli_overrides(vec![("features.plugins".to_string(), false.into())])
            .build()
            .await
            .unwrap(),
    );
    let auth = codex_login::AuthManager::shared_from_config(config.as_ref(), false)
        .await
        .expect("initialize fixture auth manager");
    let (processor, _outgoing) =
        super::message_processor_tracing_tests::build_test_processor(config, auth).await;
    let permit = processor
        .turn_admission
        .admit()
        .expect("initial request admitted");
    let first = tokio::time::Instant::now() + Duration::from_secs(35);
    processor.begin_shutdown_at_intake(first);
    assert!(processor.turn_admission.admit().is_err());
    assert_eq!(processor.intake_shutdown_deadline(), Some(first));
    assert_eq!(
        processor.command_exec_processor.sandbox_cleanup_deadline(),
        Some(first.into_std())
    );
    assert!(
        processor
            .subscribe_intake_thread_binding()
            .borrow()
            .unwrap()
            .is_complete()
    );
    processor.begin_shutdown_at_intake(first + Duration::from_secs(30));
    assert_eq!(processor.intake_shutdown_deadline(), Some(first));
    assert_eq!(
        processor.command_exec_processor.sandbox_cleanup_deadline(),
        Some(first.into_std())
    );
    drop(permit);
    assert_eq!(
        processor.shutdown_models_refresh_worker_until(first).await,
        crate::models_refresh_worker::ModelsRefreshShutdown::Joined
    );
    processor.clear_runtime_references();
}

#[tokio::test]
async fn registered_intake_callback_keeps_only_weak_processor_custody() {
    let home = TempDir::new().unwrap();
    let config = Arc::new(
        codex_core::config::ConfigBuilder::default()
            .codex_home(home.path().to_path_buf())
            .cli_overrides(vec![("features.plugins".to_string(), false.into())])
            .build()
            .await
            .unwrap(),
    );
    let auth = codex_login::AuthManager::shared_from_config(config.as_ref(), false)
        .await
        .expect("initialize fixture auth manager");
    let (processor, _outgoing) =
        super::message_processor_tracing_tests::build_test_processor(config, auth).await;
    let (latch, _custody) = crate::shutdown_intake::OwnerLatch::new();
    let latch = Arc::new(latch);
    let strong = Arc::strong_count(&processor);
    crate::standalone_shutdown::register_processor_intake(&latch, &processor);
    assert_eq!(Arc::strong_count(&processor), strong);
    crate::shutdown_intake::install_intake_latch(&latch);
    latch.expiry().mark_shutdown_edge();
    assert_eq!(
        processor.intake_shutdown_deadline(),
        latch.expiry().deadline()
    );
    assert!(processor.turn_admission.admit().is_err());
    assert_eq!(
        processor
            .shutdown_models_refresh_worker_until(latch.expiry().deadline().unwrap())
            .await,
        crate::models_refresh_worker::ModelsRefreshShutdown::Joined
    );
    processor.clear_runtime_references();
}

async fn binding_fixture() -> (TempDir, crate::ProcessorShutdownStages) {
    let home = TempDir::new().unwrap();
    let config = Arc::new(
        codex_core::config::ConfigBuilder::default()
            .codex_home(home.path().to_path_buf())
            .cli_overrides(vec![("features.plugins".to_string(), false.into())])
            .build()
            .await
            .unwrap(),
    );
    let auth = codex_login::AuthManager::shared_from_config(config.as_ref(), false)
        .await
        .unwrap();
    let (processor, _outgoing) =
        super::message_processor_tracing_tests::build_test_processor(config, auth).await;
    let stages = crate::ProcessorShutdownStages {
        processor,
        background_progress: Default::default(),
        connections: Vec::new(),
        cleanup: None,
        thread_retirement: None,
        expiry: codex_app_server_transport::OwnerExpiry::never(),
    };
    (home, stages)
}

async fn finish_binding_fixture(stages: crate::ProcessorShutdownStages) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    assert_eq!(
        stages
            .processor
            .shutdown_models_refresh_worker_until(deadline)
            .await,
        crate::models_refresh_worker::ModelsRefreshShutdown::Joined
    );
    stages.processor.clear_runtime_references();
}

#[tokio::test]
async fn bind_stage_waits_for_the_first_edge_census() {
    use crate::standalone_shutdown::StandaloneStages;
    let (_home, mut stages) = binding_fixture().await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(35);
    // The latch has published its deadline. Poll before the callback starts
    // and again between its deadline and receipt publications.
    let mut bind = stages.bind_runtimes(deadline);
    assert!(futures::poll!(&mut bind).is_pending());
    stages
        .processor
        .intake_shutdown_deadline
        .set(deadline)
        .unwrap();
    let observed = futures::poll!(&mut bind);
    assert!(
        observed.is_pending(),
        "premature bind verdict: {observed:?}"
    );
    let census = stages
        .processor
        .bind_thread_shutdown_deadlines_now(deadline);
    assert!(census.is_complete());
    stages
        .processor
        .intake_thread_binding
        .send_replace(Some(census));
    assert_eq!(futures::poll!(bind), std::task::Poll::Ready(true));
    // The same first-edge receipt is also visible if publication wins.
    assert_eq!(
        futures::poll!(stages.bind_runtimes(deadline)),
        std::task::Poll::Ready(true)
    );
    finish_binding_fixture(stages).await;
}

#[tokio::test]
async fn unproven_intake_receipt_stays_incomplete_after_clean_drain_census() {
    use crate::standalone_shutdown::StandaloneShutdownOwner;
    use crate::standalone_shutdown::StandaloneStages;
    use codex_core::ShutdownDeadlineBinding;
    for census in [
        ShutdownDeadlineBinding {
            legacy_cleanup_started: 1,
            ..Default::default()
        },
        ShutdownDeadlineBinding {
            unavailable: 1,
            ..Default::default()
        },
    ] {
        let (_home, mut stages) = binding_fixture().await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(35);
        stages
            .processor
            .intake_shutdown_deadline
            .set(deadline)
            .unwrap();
        let mut bind = stages.bind_runtimes(deadline);
        assert!(futures::poll!(&mut bind).is_pending());
        stages
            .processor
            .intake_thread_binding
            .send_replace(Some(census));
        let mut owner = StandaloneShutdownOwner::new(deadline, stages.expiry.clone());
        let clean = owner.drive("bind_runtimes", deadline, bind).await.unwrap();
        assert!(!clean);
        owner.record("bind_runtimes", clean);
        assert_eq!(
            owner.exit_report().unproven,
            Some(("incomplete_stage", "bind_runtimes"))
        );
        assert!(owner.exit_report().into_shutdown_result().is_err());
        finish_binding_fixture(stages).await;
    }
}

#[tokio::test]
async fn missing_intake_receipt_exhausts_original_deadline_and_late_success_cannot_clear_it() {
    use crate::standalone_shutdown::StandaloneShutdownOwner;
    use crate::standalone_shutdown::StandaloneStages;
    let (_home, mut stages) = binding_fixture().await;
    tokio::time::pause();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(35);
    stages
        .processor
        .intake_shutdown_deadline
        .set(deadline)
        .unwrap();
    let mut owner = StandaloneShutdownOwner::new(deadline, stages.expiry.clone());
    let mut drive =
        Box::pin(owner.drive("bind_runtimes", deadline, stages.bind_runtimes(deadline)));
    assert!(futures::poll!(&mut drive).is_pending());
    tokio::time::advance(Duration::from_secs(35)).await;
    assert_eq!(drive.await, None);
    stages
        .processor
        .intake_thread_binding
        .send_replace(Some(Default::default()));
    assert_eq!(
        owner.exit_report().unproven,
        Some(("timed_out_stage", "bind_runtimes"))
    );
    assert!(owner.exit_report().into_shutdown_result().is_err());
    tokio::time::resume();
    finish_binding_fixture(stages).await;
}

#[tokio::test]
async fn connection_drain_timeout_remains_unclean_after_original_finishes() {
    let home = TempDir::new().unwrap();
    let config = Arc::new(
        codex_core::config::ConfigBuilder::default()
            .codex_home(home.path().to_path_buf())
            .cli_overrides(vec![("features.plugins".to_string(), false.into())])
            .build()
            .await
            .unwrap(),
    );
    let auth = codex_login::AuthManager::shared_from_config(config.as_ref(), false)
        .await
        .expect("initialize fixture auth manager");
    let (processor, _outgoing) =
        super::message_processor_tracing_tests::build_test_processor(config, auth).await;
    let session = super::ConnectionSessionState::new(crate::transport::ConnectionOrigin::Stdio);
    let gate = Arc::clone(&session.rpc_gate);
    let ticket = crate::connection_rpc_gate::ConnectionRpcGate::ticket(&gate);
    let (started, running) = tokio::sync::oneshot::channel();
    let (release, blocked) = tokio::sync::oneshot::channel();
    let original = tokio::spawn(async move {
        gate.run_admitted(ticket, async move {
            started.send(()).unwrap();
            blocked.await.unwrap();
        })
        .await;
    });
    running.await.unwrap();
    processor
        .connection_closed_until(
            crate::outgoing_message::ConnectionId(7),
            &session,
            Some(tokio::time::Instant::now()),
        )
        .await;
    assert!(!processor.connection_rpc_drains_complete());
    assert!(!original.is_finished());
    release.send(()).unwrap();
    original.await.unwrap();
    processor
        .connection_closed(crate::outgoing_message::ConnectionId(7), &session)
        .await;
    assert!(!processor.connection_rpc_drains_complete());
    processor.clear_runtime_references();
}

#[tokio::test]
async fn connection_drain_preserves_handler_panic_and_half_close_unanswered() {
    let home = TempDir::new().unwrap();
    let config = Arc::new(
        codex_core::config::ConfigBuilder::default()
            .codex_home(home.path().to_path_buf())
            .cli_overrides(vec![("features.plugins".to_string(), false.into())])
            .build()
            .await
            .unwrap(),
    );
    let auth = codex_login::AuthManager::shared_from_config(config.as_ref(), false)
        .await
        .expect("initialize fixture auth manager");
    let (processor, _outgoing) =
        super::message_processor_tracing_tests::build_test_processor(config, auth).await;
    let session = super::ConnectionSessionState::new(crate::transport::ConnectionOrigin::Stdio);
    let ticket = crate::connection_rpc_gate::ConnectionRpcGate::ticket(&session.rpc_gate);
    session
        .rpc_gate
        .run_admitted(ticket, async { panic!("handler failure witness") })
        .await;
    // A frame read but never admitted/refused is still owed a reply at EOF.
    let unanswered = crate::connection_rpc_gate::ConnectionRpcGate::ticket(&session.rpc_gate);
    drop(unanswered);
    processor
        .connection_half_closed_until(
            crate::outgoing_message::ConnectionId(7),
            &session,
            Some(tokio::time::Instant::now() + Duration::from_secs(1)),
        )
        .await;
    assert!(!processor.connection_rpc_drains_complete());
    assert_eq!(processor.unanswered_after_half_close(), 1);
    processor.clear_runtime_references();
}

#[tokio::test]
async fn not_admitted_refusal_reports_exact_recipient_and_router_loss() {
    use crate::outgoing_message::ConnectionId;
    use crate::outgoing_message::OutgoingEnvelope;
    use crate::outgoing_message::OutgoingMessage;
    use codex_app_server_protocol::RequestId;

    let home = TempDir::new().unwrap();
    let config = Arc::new(
        codex_core::config::ConfigBuilder::default()
            .codex_home(home.path().to_path_buf())
            .cli_overrides(vec![("features.plugins".to_string(), false.into())])
            .build()
            .await
            .unwrap(),
    );
    let auth = codex_login::AuthManager::shared_from_config(config.as_ref(), false)
        .await
        .expect("initialize fixture auth manager");
    let (processor, mut outgoing) =
        super::message_processor_tracing_tests::build_test_processor(config, auth).await;
    assert!(
        tokio::time::timeout(
            Duration::from_secs(1),
            processor.refuse_not_admitted(ConnectionId(41), RequestId::Integer(73)),
        )
        .await
        .expect("refusal must finish")
    );
    let envelope = tokio::time::timeout(Duration::from_secs(1), outgoing.recv())
        .await
        .unwrap()
        .unwrap();
    match envelope {
        OutgoingEnvelope::ToConnection {
            connection_id,
            message: OutgoingMessage::Error(error),
            ..
        } => {
            assert_eq!(connection_id, ConnectionId(41));
            assert_eq!(error.id, RequestId::Integer(73));
            assert_eq!(error.error.message, super::NOT_ADMITTED_MESSAGE);
        }
        _ => panic!("refusal must retain the request and recipient"),
    }
    drop(outgoing);
    assert!(
        !tokio::time::timeout(
            Duration::from_secs(1),
            processor.refuse_not_admitted(ConnectionId(41), RequestId::Integer(74)),
        )
        .await
        .expect("closed router must refuse promptly")
    );
    processor.clear_runtime_references();
}
