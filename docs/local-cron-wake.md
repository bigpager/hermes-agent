# Fixed local cron wake patch

This branch is based on 03b0c7947262b220f5148b75a30bb7a3faddcbb2, already present in the fork's upstream ancestry. It contains only the local-wake component and its tests, plus this technical note. It is not an upstream release or a general-purpose remote execution API.

The Unix control verb `wake-generic-intake` accepts no parameters and requires a kernel-verified same-account peer. Only the default-home gateway registers it. Admission reserves drain accounting on the gateway event loop before the native scheduler's durable claim; disabled/busy jobs and ESTOP remain authoritative. Cancellation of a waiting coroutine cannot release ownership while its underlying thread is still active.

The existing job snapshot gets an event-only pre-script and no monitor, without persisting these overrides. Native scheduler execution is retained. Shutdown and idle accounting include the local admission reservation. No network listener or credential is added.

The fixed `JOB_ID` is intentionally preserved byte-for-byte. It is an opaque local selector, not an authentication token, repository identifier, URL, or grant of authority. Knowing it does not bypass Unix peer authentication, account ownership, job enabled state or native CAS. No private job definition, repository mapping, host path, operator identity, payload or execution record accompanies it. Other installations must review their own target before using this deployment-specific integration; it is not auto-configuring.

## Verification

Run the prescribed runner from this checkout:

    scripts/run_tests.sh tests/gateway/test_local_cron_wake.py tests/gateway/test_control_socket.py tests/gateway/test_cron_fire_webhook.py tests/gateway/test_external_drain_control.py tests/gateway/test_api_server_active_work_drain.py tests/gateway/test_cron_active_work_drain.py tests/gateway/test_scale_to_zero.py tests/tools/test_cronjob_run_immediate.py tests/cron/test_scheduler_provider.py tests/cron/test_execution_ledger.py

The temporary native job store and real Unix socket are exercised; the agent body is stubbed. This is offline integration coverage, not a fresh production agent execution. Same-account processes remain cooperative, not sandboxed.

## Maintenance gate

Before an upgrade, compare native claim/fire, profile scope, drain reservation, shutdown and control socket APIs against this patch. Re-run the suite on an isolated checkout before touching the running tree. Require authorized admission gating, graceful drain of connections/agents/cron/claims, private backups and a reviewed recovery path before replacing source or restarting anything. Preserve queue generations, pending wake intent, attempts and uncertain outcomes. Do not restore an old queue over newer work.

Replace this patch only when an upstream mechanism demonstrates same-account fixed-target admission, native durable CAS, disabled/ESTOP enforcement, event-only selection, profile binding, cancellation ownership and drain/idle accounting with equivalent tests. Remove the integration and its callers together after a verified migration; do not merely delete the verb while producers still rely on it.
