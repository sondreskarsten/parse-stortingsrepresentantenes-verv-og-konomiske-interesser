"""CLI interface for stortinget-register.

Commands:
    sync        Discover and download missing register PDFs
    status      Show manifest statistics and checkpoint state
"""

from __future__ import annotations

import asyncio

import structlog
import typer
from rich.console import Console

from stortinget_register.config import Settings

app = typer.Typer(
    name="stortinget-register",
    help="Mirror of Stortinget economic interests register PDFs.",
    no_args_is_help=True,
)
console = Console()


def _configure_logging(level: str) -> None:
    import logging

    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, level.upper(), logging.INFO),
    )
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.dev.ConsoleRenderer(),
        ],
    )


@app.command()
def sync(
    storage_path: str = typer.Argument(
        ..., help="Root storage path (local, s3://, or gs://)"
    ),
    max_concurrent: int = typer.Option(5, "--max-concurrent", "-c"),
    max_runtime: int = typer.Option(
        0, "--max-runtime", help="Max runtime in minutes (0=unlimited)"
    ),
    scan_start_year: int = typer.Option(2021, "--start-year"),
    scan_end_year: int | None = typer.Option(None, "--end-year"),
    log_level: str = typer.Option("INFO", "--log-level", "-l"),
) -> None:
    """Discover and download missing Stortinget register PDFs."""
    _configure_logging(log_level)

    settings = Settings(
        storage_path=storage_path,
        max_concurrent=max_concurrent,
        max_runtime_minutes=max_runtime,
        scan_start_year=scan_start_year,
        scan_end_year=scan_end_year,
        log_level=log_level,
    )

    from stortinget_register.downloader import SyncEngine

    engine = SyncEngine(settings)
    asyncio.run(engine.run())


@app.command()
def status(
    storage_path: str = typer.Argument(
        ..., help="Root storage path (local, s3://, or gs://)"
    ),
) -> None:
    """Show manifest statistics and checkpoint state."""
    settings = Settings(storage_path=storage_path)
    _configure_logging(settings.log_level)

    from stortinget_register.checkpoint import CheckpointManager
    from stortinget_register.manifest import ManifestManager
    from stortinget_register.storage import StorageBackend

    storage = StorageBackend.from_settings(settings)
    manifest = ManifestManager(storage, settings.manifest_path)
    checkpoint = CheckpointManager(storage, settings.checkpoint_path)

    table = manifest.load()
    state = checkpoint.load()

    console.print(f"[bold]Manifest:[/bold] {settings.manifest_path}")
    console.print(f"  Total records: {table.num_rows}")
    if table.num_rows > 0:
        import pyarrow.compute as pc

        status_col = table.column("status")
        for s in ["success", "failed", "pending"]:
            count = pc.sum(pc.equal(status_col, s)).as_py()
            if count:
                console.print(f"  {s}: {count}")

        date_col = table.column("date")
        success_mask = pc.equal(status_col, "success")
        success_dates = table.filter(success_mask).column("date")
        if success_dates.length() > 0:
            dates_list = sorted(success_dates.to_pylist())
            console.print(f"  Date range: {dates_list[0]} → {dates_list[-1]}")

        folders = set(table.column("period_folder").to_pylist())
        folders.discard(None)
        if folders:
            console.print(f"  Period folders: {', '.join(sorted(folders))}")

    console.print(f"\n[bold]Checkpoint:[/bold] {settings.checkpoint_path}")
    console.print(f"  Last date scanned: {state.last_date_scanned}")
    console.print(f"  Dates scanned: {state.dates_scanned}")
    console.print(f"  PDFs found: {state.pdfs_found}")
    console.print(f"  PDFs downloaded: {state.pdfs_downloaded}")
    console.print(f"  Errors: {state.errors}")


if __name__ == "__main__":
    app()
