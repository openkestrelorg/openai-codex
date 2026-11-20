use super::*;
use codex_extension_api::ExtensionData;
use codex_extension_api::ExtensionFuture;
use codex_extension_api::TurnStartAdmission;
use codex_extension_api::TurnWorkRefused;
use codex_protocol::host_turn_work::HostTurnWork;
use pretty_assertions::assert_eq;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

#[derive(Debug)]
struct ParentGate {
    parent_store: String,
    parent_turn: String,
    child_store: String,
    allow: bool,
    derived: AtomicUsize,
    fresh: AtomicUsize,
    bound: Arc<AtomicUsize>,
}

#[derive(Debug)]
struct ChildWork(Arc<AtomicUsize>);

impl HostTurnWork for ChildWork {
    fn bind_submission(&mut self, _: &str) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn retain_until_terminal(self: Box<Self>) {}
}

impl TurnStartAdmission for ParentGate {
    fn admit_turn_start(&self) -> Option<Box<dyn Send>> {
        self.fresh.fetch_add(1, Ordering::SeqCst);
        None
    }

    fn requires_account_work(&self) -> bool {
        true
    }

    fn admit_turn_work(
        &self,
        _: &ExtensionData,
        _: ExtensionFuture<'static, ()>,
    ) -> Result<Option<Box<dyn HostTurnWork>>, TurnWorkRefused> {
        self.fresh.fetch_add(1, Ordering::SeqCst);
        Err(TurnWorkRefused::Unavailable)
    }

    fn derive_turn_work(
        &self,
        parent: &ExtensionData,
        turn: &str,
        child: &ExtensionData,
        _: ExtensionFuture<'static, ()>,
    ) -> Result<Option<Box<dyn HostTurnWork>>, TurnWorkRefused> {
        assert_eq!(parent.level_id(), self.parent_store);
        assert_eq!(turn, self.parent_turn);
        assert_eq!(child.level_id(), self.child_store);
        self.derived.fetch_add(1, Ordering::SeqCst);
        if !self.allow {
            return Err(TurnWorkRefused::Unavailable);
        }
        Ok(Some(Box::new(ChildWork(Arc::clone(&self.bound)))))
    }
}

async fn send_with_parent(allow: bool) {
    let (mut session, turn) = make_session_and_context().await;
    let manager = thread_manager();
    set_agent_control(&mut session, manager.agent_control());
    let target = manager
        .start_thread(StartThreadOptions::new(
            turn.config.as_ref().clone(),
            /*control_endpoint*/ None,
        ))
        .await
        .expect("start target before installing parent admission policy");
    let gate = Arc::new(ParentGate {
        parent_store: session.services.thread_extension_data.level_id().to_owned(),
        parent_turn: turn.sub_id.clone(),
        child_store: target
            .thread
            .session
            .services
            .thread_extension_data
            .level_id()
            .to_owned(),
        allow,
        derived: AtomicUsize::new(0),
        fresh: AtomicUsize::new(0),
        bound: Arc::new(AtomicUsize::new(0)),
    });
    session.services.host_admission = Some(gate.clone());
    let result = SendInputHandler
        .handle(invocation(
            Arc::new(session),
            Arc::new(turn),
            "send_input",
            function_payload(
                json!({"target": target.thread_id.to_string(), "message": "parent input"}),
            ),
        ))
        .await;
    assert_eq!(gate.derived.load(Ordering::SeqCst), 1);
    assert_eq!(gate.fresh.load(Ordering::SeqCst), 0);
    if allow {
        result.expect("a live parent derives work despite closed fresh parent admission");
        wait_for_recorded_user_input(
            target.thread.as_ref(),
            &[UserInput::Text {
                text: "parent input".into(),
                text_elements: Vec::new(),
            }],
        )
        .await;
        assert_eq!(gate.bound.load(Ordering::SeqCst), 1);
    } else {
        assert!(
            result.is_err(),
            "a refused parent must not fall back to the hostless target"
        );
        assert_eq!(gate.bound.load(Ordering::SeqCst), 0);
    }
    target
        .thread
        .shutdown_and_wait()
        .await
        .expect("shutdown target");
}

#[tokio::test]
async fn send_input_uses_exact_parent_work_with_fresh_parent_admission_closed() {
    send_with_parent(true).await;
}

#[tokio::test]
async fn send_input_refuses_parent_failure_instead_of_fresh_child_admission() {
    send_with_parent(false).await;
}
