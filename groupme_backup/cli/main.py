"""Main CLI entry point."""

import logging
import sys

import click
from rich.console import Console
from rich.logging import RichHandler

from ..config.settings import get_settings

console = Console()


def setup_logging(verbose: bool = False) -> None:
    """Set up logging with rich handler."""
    level = logging.DEBUG if verbose else logging.INFO

    logging.basicConfig(
        level=level,
        format="%(message)s",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Enable verbose logging")
@click.pass_context
def cli(ctx: click.Context, verbose: bool) -> None:
    """GroupMe Backup & Analytics Tool

    A CLI tool to backup GroupMe group chats to PostgreSQL with analytics capabilities.
    """
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    setup_logging(verbose)

    # Validate settings early
    try:
        settings = get_settings()
        ctx.obj["settings"] = settings
    except Exception as e:
        console.print(f"[red]Error loading configuration: {e}[/red]")
        console.print(
            "\n[yellow]Please ensure your .env file is configured correctly.[/yellow]"
        )
        console.print("See .env.example for reference.")
        sys.exit(1)


@cli.command()
def version() -> None:
    """Show version information."""
    console.print("[bold]GroupMe Backup Tool[/bold] version 0.1.0")
    console.print("https://github.com/spaceisawaste/groupme-backup")


# Import command modules to register them with the CLI
from . import analytics  # noqa: F401, E402
from . import backup  # noqa: F401, E402


if __name__ == "__main__":
    cli(obj={})
