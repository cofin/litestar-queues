"""Command-line interface for reproducible queue benchmarks."""

import argparse
import json
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

from tools.dev_infra import InfraError
from tools.queue_bench.models import BenchmarkResult
from tools.queue_bench.profiles import BACKEND_VARIANTS, PROFILE_NAMES, SCENARIOS, parse_parameter_overrides
from tools.queue_bench.report import render_markdown
from tools.queue_bench.runner import DEFAULT_SYSTEMS, SYSTEM_BACKENDS, RunConfig, run_benchmarks


def build_parser() -> argparse.ArgumentParser:
    """Build the benchmark command parser.

    Returns:
        Configured top-level parser.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run", help="run benchmark samples")
    run.add_argument("--system", action="append", choices=sorted(SYSTEM_BACKENDS))
    run.add_argument("--backend", action="append", choices=("redis", "postgres", "valkey"))
    run.add_argument("--scenario", action="append", choices=SCENARIOS)
    run.add_argument("--profile", choices=PROFILE_NAMES, default="core")
    run.add_argument("--backend-variant", choices=BACKEND_VARIANTS, default="default")
    run.add_argument("--parameter", action="append", default=[], metavar="KEY=JSON")
    run.add_argument("--warmups", type=int, default=3)
    run.add_argument("--samples", type=int, default=10)
    run.add_argument("--operations", type=int, default=100)
    run.add_argument("--seed", type=int, default=20260720)
    run.add_argument("--payload-size", type=int, default=512)
    run.add_argument("--concurrency", type=int, default=1)
    run.add_argument("--dsn", action="append", default=[], metavar="BACKEND=URL")
    run.add_argument("--pull-images", action="store_true")
    run.add_argument("--remote", action="store_true")
    run.add_argument("--acknowledge-cost", action="store_true")
    run.add_argument("--managed-namespace")
    run.add_argument("--google-project")
    credentials = run.add_mutually_exclusive_group()
    credentials.add_argument("--google-credentials-file", type=Path)
    credentials.add_argument("--google-adc", action="store_true")
    run.add_argument("--cold-state-evidence", type=Path)
    run.add_argument("--cloud-tasks-location")
    run.add_argument("--cloud-tasks-queue")
    run.add_argument("--cloud-tasks-service-url")
    run.add_argument("--cloud-tasks-service-account")
    run.add_argument("--cloud-tasks-audience")
    run.add_argument("--cloud-run-region")
    run.add_argument("--cloud-run-job")
    run.add_argument("--timeout", type=float, default=120.0, dest="timeout_seconds")
    run.add_argument("--output", type=Path, required=True)

    report = subparsers.add_parser("report", help="render an existing JSON result")
    report.add_argument("result", type=Path)
    report.add_argument("--format", choices=("markdown", "json"), default="markdown")
    report.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the benchmark CLI.

    Returns:
        Process exit status.
    """
    args = build_parser().parse_args(argv)
    try:
        if args.command == "run":
            config = RunConfig(
                systems=tuple(args.system or DEFAULT_SYSTEMS),
                backends=tuple(args.backend or ("redis", "postgres")),
                scenarios=tuple(args.scenario or _default_scenarios(args.profile)),
                profile=args.profile,
                backend_variant=args.backend_variant,
                parameters=parse_parameter_overrides(args.parameter),
                warmups=args.warmups,
                samples=args.samples,
                operations=args.operations,
                payload_size=args.payload_size,
                concurrency=args.concurrency,
                seed=args.seed,
                dsn_overrides=tuple(args.dsn),
                pull_images=args.pull_images,
                remote=args.remote,
                acknowledge_cost=args.acknowledge_cost,
                managed_namespace=args.managed_namespace,
                google_project=args.google_project,
                google_credentials_file=args.google_credentials_file,
                google_adc=args.google_adc,
                cold_state_evidence=args.cold_state_evidence,
                cloud_tasks_location=args.cloud_tasks_location,
                cloud_tasks_queue=args.cloud_tasks_queue,
                cloud_tasks_service_url=args.cloud_tasks_service_url,
                cloud_tasks_service_account=args.cloud_tasks_service_account,
                cloud_tasks_audience=args.cloud_tasks_audience,
                cloud_run_region=args.cloud_run_region,
                cloud_run_job=args.cloud_run_job,
                timeout_seconds=args.timeout_seconds,
            )
            result = run_benchmarks(config, root=Path.cwd())
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n")
            invalid = sum(not sample.valid for sample in result.samples)
            print(f"wrote {len(result.samples)} samples to {args.output} ({invalid} invalid)")
            return 1 if invalid else 0
        result = BenchmarkResult.from_dict(json.loads(args.result.read_text()))
        rendered = render_markdown(result) if args.format == "markdown" else json.dumps(result.to_dict(), indent=2)
        if args.output is not None:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(rendered + ("" if rendered.endswith("\n") else "\n"))
        else:
            print(rendered)
    except (InfraError, OSError, ValueError, json.JSONDecodeError, subprocess.SubprocessError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    else:
        return 0


def _default_scenarios(profile: str) -> tuple[str, ...]:
    if profile == "cloud-tasks":
        return ("cloud-tasks-delivery",)
    if profile == "cloud-run-jobs":
        return ("cloud-run-job-dispatch",)
    if profile in {"heartbeat", "events"}:
        return (profile,)
    if profile == "uniqueness":
        return ("enqueue",)
    if profile == "maintenance":
        return ("terminal-retention", "event-retention", "lease-contention")
    return ("enqueue", "roundtrip")


if __name__ == "__main__":
    raise SystemExit(main())
