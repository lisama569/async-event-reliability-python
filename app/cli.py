"""Local entry points: API server and standalone consumer processes."""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal

import uvicorn

from .config import load_settings
from .processor import EventProcessor
from .sideeffects import SideEffectGateway
from .storage import EventStore
from .worker import Worker


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def run_serve(settings, host: str, port: int,
              start_worker: bool = True) -> None:
    # Import lazily so `uvicorn`-only deployments keep a single import surface.
    from .api import create_app

    app = create_app(settings, start_worker=start_worker)
    uvicorn.run(app, host=host, port=port)


def run_worker(settings, worker_id: str | None = None) -> None:
    store = EventStore(settings.db_path, tenant_quota=settings.tenant_quota)
    processor = EventProcessor(SideEffectGateway(store))
    worker = Worker(store, processor, settings, worker_id=worker_id)

    async def _main() -> None:
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()

        def _signal_handler() -> None:
            worker.stop()
            stop_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _signal_handler)
            except NotImplementedError:  # Windows
                pass

        task = asyncio.create_task(worker.run())
        await stop_event.wait()
        await task

    asyncio.run(_main())


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="event-system",
        description="Local async event inbound + reliable processing demo")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="run the API server (+in-proc worker)")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--no-worker", action="store_true",
                       help="do not start the in-process consumer; use this "
                            "with standalone `worker` processes")

    work = sub.add_parser("worker", help="run a standalone consumer process")
    work.add_argument("--id", default=None, help="explicit worker id")

    args = parser.parse_args(argv)
    _setup_logging(args.verbose)
    settings = load_settings()

    if args.command == "serve":
        run_serve(settings, args.host, args.port,
                  start_worker=not args.no_worker)
    elif args.command == "worker":
        run_worker(settings, worker_id=args.id)


if __name__ == "__main__":
    main()
