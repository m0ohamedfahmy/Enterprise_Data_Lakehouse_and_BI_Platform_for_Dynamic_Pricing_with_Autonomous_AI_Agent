#!/usr/bin/env python3
"""
lakehouse_ai_agent.py — Autonomous AI Data Platform Agent (SRE + Data Quality Guard)

Continuously polls an Astro/Airflow DAG's runs, and for every failed task
instance it discovers, runs the Mandatory Operational Sequence:
  1. Read task logs first (AirflowClient.build_task_failures)
  2. Evaluate failure type: infra/orchestration issues are diagnosed and
     (optionally) retried directly, with no script code read; only an
     explicit script-level execution error proceeds further
  3. For script-level failures, resolve + read ONLY the single failing
     script from its `.env`-configured base dir (INGESTION_SCRIPTS_DIR /
     DBT_PROJECT_DIR / ML_SCRIPTS_DIR)
  4. Generate a root-cause diagnosis, write an RCA report to
     ./rca_reports/, and — non-destructively — write a corrected version
     of that script to ./recommended_fixes/ (Shadow Code Generation;
     recommended_action stays alert_only, nothing is auto-applied)

It also runs lightweight proactive guards each cycle (Nessie branch health,
MinIO model/scoring artifact freshness) independent of Airflow task state.

Usage:
    python lakehouse_ai_agent.py --dag-id lakehouse_pipeline --poll-interval 30
    python lakehouse_ai_agent.py --dag-id lakehouse_pipeline --auto-heal
    python lakehouse_ai_agent.py --dag-id lakehouse_pipeline --once   # single cycle, for cron/CI
"""

from __future__ import annotations

import sys
import time

from dotenv import load_dotenv

from lakehouse_agent.agent_graph import build_agent_graph, run_failure_through_graph
from lakehouse_agent.clients.airflow_client import AirflowClient
from lakehouse_agent.clients.docker_client import DockerArtifactClient
from lakehouse_agent.clients.nessie_client import NessieClient
from lakehouse_agent.config import AgentConfig, build_config_from_cli
from lakehouse_agent.console import console, log_diagnosis, log_error, log_info, log_success, log_warn
from lakehouse_agent.diagnosis_engine import DiagnosisEngine
from lakehouse_agent.groq_router import GroqRouter
from lakehouse_agent.guards import run_ingestion_guard, run_ml_guard
from lakehouse_agent.remediation_engine import RemediationEngine

# in-memory retry counters, keyed by (dag_id, run_id, task_id) — resets on
# restart by design; persistent state would move this to a small SQLite/Redis
# store, deliberately out of scope for a host-process agent like this one.
_RETRY_COUNTS: dict[tuple[str, str, str], int] = {}


def _validate_config(config: AgentConfig) -> None:
    if not config.groq_api_key:
        log_error("GROQ_API_KEY is not set (env var or .env file). The agent cannot reach GroqCloud without it.")
        sys.exit(1)


def run_cycle(config: AgentConfig, airflow: AirflowClient, nessie: NessieClient, docker_artifacts: DockerArtifactClient, graph_app) -> None:
    console.rule(f"[bold]Poll cycle — DAG '{config.dag_id}'[/]")

    try:
        dag_runs = airflow.get_latest_dag_runs(config.dag_id, limit=1)
    except Exception as e:  # noqa: BLE001
        log_error(f"Could not reach Airflow REST API at {config.airflow_base_url}: {e}")
        return

    if not dag_runs:
        log_info(f"No recent DAG runs found for '{config.dag_id}'.")
        return

    any_failures = False
    for dag_run in dag_runs:
        run_id = dag_run["dag_run_id"]
        state = dag_run.get("state")
        log_info(f"Run {run_id}: state={state}")

        if state != "failed":
            continue

        # Step 1: read task logs first — build_task_failures fetches each
        # failed task instance's log tail before any diagnosis happens.
        failures = airflow.build_task_failures(config.dag_id, run_id)
        if not failures:
            continue

        any_failures = True
        for failure in failures:
            key = (failure.dag_id, failure.run_id, failure.task_id)
            retries_so_far = _RETRY_COUNTS.get(key, 0)

            log_warn(f"Failed task detected: {failure.dag_id}.{failure.task_id} (run {failure.run_id}, stage={failure.stage.value})")
            event = run_failure_through_graph(graph_app, failure, retries_attempted=retries_so_far)

            if event.diagnosis and event.diagnosis.fix_file_path:
                log_success(f"Shadow fix (non-destructive): {event.diagnosis.fix_file_path}")
            if event.remediation:
                _RETRY_COUNTS[key] = event.retries_attempted
                status = "✔" if event.remediation.success else "✖"
                log_info(f"{status} Remediation: {event.remediation.action_taken.value} — {event.remediation.detail}")
            if event.rca_report_path:
                log_success(f"RCA report written: {event.rca_report_path}")

        # Proactive ML guard for this run's artifacts, regardless of which
        # specific task failed — catches silent artifact issues too.
        run_ml_guard(docker_artifacts)

    run_ingestion_guard(nessie)

    if not any_failures:
        log_success("No failed task instances in recent runs.")


def main() -> None:
    load_dotenv()
    config, run_once = build_config_from_cli()
    _validate_config(config)

    console.print(
        f"[bold cyan]Lakehouse AI Agent[/] starting — dag_id=[bold]{config.dag_id}[/] "
        f"auto_heal=[bold]{config.auto_heal}[/] poll_interval=[bold]{config.poll_interval_seconds}s[/]"
    )

    # AgentConfig (built above via build_config_from_cli/.env) already
    # carries the new script-source paths — INGESTION_SCRIPTS_DIR,
    # SPARK_JOBS_DIR, DBT_PROJECT_DIR, ML_SCRIPTS_DIR, and
    # RECOMMENDED_FIXES_DIR — so every client/engine below shares the same
    # validated `config` object instead of re-reading the environment.
    airflow = AirflowClient(config)
    nessie = NessieClient(config)
    docker_artifacts = DockerArtifactClient(config)
    router = GroqRouter(config)
    diagnosis_engine = DiagnosisEngine(router, config)
    remediation_engine = RemediationEngine(config, airflow, nessie)
    # `router` is passed alongside `config` so report_node can call
    # rca_generator.generate_shadow_fix_and_rca(config, router, ...).
    graph_app = build_agent_graph(config, router, diagnosis_engine, remediation_engine)

    if run_once:
        run_cycle(config, airflow, nessie, docker_artifacts, graph_app)
        return

    try:
        while True:
            run_cycle(config, airflow, nessie, docker_artifacts, graph_app)
            time.sleep(config.poll_interval_seconds)
    except KeyboardInterrupt:
        console.print("\n[bold yellow]Shutting down — goodbye.[/]")


if __name__ == "__main__":
    main()
