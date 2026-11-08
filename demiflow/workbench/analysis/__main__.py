"""Closed external-helper entrypoint. No arbitrary command or host fallback."""
from __future__ import annotations

import argparse
import json
import signal
import sys

from demiflow.forge.schema import ForgeError
from demiflow.workbench.analysis.deployment import Deployment
from demiflow.workbench.analysis.deployment import inspect_binding


def main(argv=None):
    arguments = sys.argv[1:] if argv is None else argv
    if arguments and arguments[0] in {"supervisor", "supervisor-child"}:
        from demiflow.workbench.analysis.supervisor import main as supervised_main
        return supervised_main(arguments)
    parser = argparse.ArgumentParser(prog="demiflow-analysis-helper")
    parser.add_argument("operation", choices=("inspect-binding", "preflight", "controller", "watchdog"))
    parser.add_argument("--binding", required=True)
    parser.add_argument("--mode", choices=("production", "calibration"), default="production")
    parser.add_argument('--execution-backend', choices=('oci','trusted_local'), default='oci')
    args = parser.parse_args(argv)
    try:
        if args.execution_backend == 'trusted_local':
            from demiflow.workbench.analysis.trusted_binding import TrustedBinding
            if args.mode != 'production' or args.operation not in {'preflight','controller'}:
                raise ForgeError('Trusted mode supports its fixed preflight/controller entry only', code='operation_rejected')
            deployment = TrustedBinding.load(args.binding)
            if args.operation == 'preflight':
                print(json.dumps(dict(metadata_preflight=True,execution_mode='trusted_local',
                    origin=deployment.local_site.origin(),jobs_started=False,
                    site_controls_changed=False),ensure_ascii=False))
                return 0
        elif args.operation == "inspect-binding":
            print(json.dumps(inspect_binding(args.binding), ensure_ascii=False))
            return 0
        elif args.operation in {"controller", "watchdog"}:
            raise ForgeError("Start the fixed pair through the admitted outer supervisor", code="operation_rejected")
        else:
            deployment = Deployment.load(args.binding, mode=args.mode, role='controller')
        if args.operation == "preflight":
            print(json.dumps(dict(metadata_preflight=True, mode=args.mode, jobs_started=False,
                site_controls_changed=False, isolation_calibrated=False), ensure_ascii=False))
            return 0
        if args.operation == "watchdog":
            from demiflow.workbench.analysis.watchdog_service import WatchdogService
            service = WatchdogService(deployment)
        else:
            from demiflow.workbench.analysis.controller import Controller
            service = Controller(deployment)
        signal.signal(signal.SIGTERM, lambda *_: setattr(service, "running", False))
        signal.signal(signal.SIGINT, lambda *_: setattr(service, "running", False))
        try:
            service.serve()
        finally:
            service.close()
        return 0
    except (ForgeError, OSError) as exc:
        print(json.dumps(dict(error=str(exc)[:1024], code=getattr(exc, "code", "operation_failed")), ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
