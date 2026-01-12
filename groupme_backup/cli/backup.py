"""Backup CLI commands."""

import logging

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from ..api.client import GroupMeClient
from ..db.session import get_session
from ..sync.engine import SyncEngine
from .main import cli

logger = logging.getLogger(__name__)
console = Console()


@cli.command()
@click.option("--group-id", help="Specific group ID to backup")
@click.option("--all", "backup_all", is_flag=True, help="Backup all groups")
@click.pass_context
def backup(ctx: click.Context, group_id: str | None, backup_all: bool) -> None:
    """Backup GroupMe messages to database.

    Use --group-id to backup a specific group, or --all to backup all groups.
    """
    if not group_id and not backup_all:
        console.print("[red]Error: Please specify --group-id or --all[/red]")
        console.print("Example: groupme-backup backup --group-id 12345")
        console.print("         groupme-backup backup --all")
        return

    settings = ctx.obj["settings"]
    api_client = GroupMeClient(
        access_token=settings.groupme_access_token,
        base_url=settings.groupme_api_base_url,
        rate_limit_calls=settings.groupme_rate_limit_calls,
        rate_limit_period=settings.groupme_rate_limit_period,
    )

    with get_session() as session:
        sync_engine = SyncEngine(api_client, session)

        if backup_all:
            console.print("[bold blue]Fetching all groups...[/bold blue]")

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task("Fetching groups...", total=None)
                groups = api_client.get_all_groups()
                progress.update(task, completed=True)

            # Filter by backup_group_ids if configured
            if settings.backup_group_ids:
                groups = [g for g in groups if g["id"] in settings.backup_group_ids]
                console.print(
                    f"\n[yellow]Filtered to {len(groups)} configured group(s)[/yellow]"
                )

            console.print(f"\nFound [bold]{len(groups)}[/bold] groups to backup")

            # Show table of groups
            table = Table(title="Groups to Backup")
            table.add_column("Group ID", style="cyan")
            table.add_column("Name", style="green")
            table.add_column("Members", justify="right")

            for group in groups[:10]:  # Show first 10
                members = group.get("members")
                member_count = members.get("count") if members and isinstance(members, dict) else "?"
                table.add_row(
                    group["id"],
                    group.get("name", "Unknown"),
                    str(member_count),
                )

            if len(groups) > 10:
                table.add_row("...", f"(and {len(groups) - 10} more)", "")

            console.print(table)
            console.print()

            # Sync all groups
            with Progress(console=console) as progress:
                task = progress.add_task(
                    "[cyan]Syncing groups...", total=len(groups)
                )

                for group in groups:
                    progress.update(
                        task, description=f"[cyan]Syncing {group['name'][:30]}..."
                    )
                    messages_count, error = sync_engine.sync_group_with_retry(
                        group["id"]
                    )

                    if error:
                        console.print(
                            f"[red]✗[/red] {group['name']}: {error}"
                        )
                    else:
                        console.print(
                            f"[green]✓[/green] {group['name']}: "
                            f"{messages_count} new messages"
                        )

                    progress.update(task, advance=1)

            console.print("\n[bold green]Backup complete![/bold green]")

        else:
            # Backup single group
            console.print(f"[bold blue]Backing up group {group_id}...[/bold blue]")

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task("Syncing messages...", total=None)
                messages_count, error = sync_engine.sync_group_with_retry(group_id)
                progress.update(task, completed=True)

            if error:
                console.print(f"\n[red]Error:[/red] {error}")
            else:
                console.print(
                    f"\n[green]Success![/green] Fetched {messages_count} new messages"
                )


@cli.command("list-groups")
@click.option("--limit", default=None, type=int, help="Limit number of groups shown")
@click.pass_context
def list_groups(ctx: click.Context, limit: int | None) -> None:
    """List all available GroupMe groups."""
    settings = ctx.obj["settings"]
    api_client = GroupMeClient(
        access_token=settings.groupme_access_token,
        base_url=settings.groupme_api_base_url,
    )

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Fetching groups...", total=None)
        groups = api_client.get_all_groups()
        progress.update(task, completed=True)

    if limit:
        groups = groups[:limit]

    # Create table
    table = Table(title=f"GroupMe Groups ({len(groups)} total)")
    table.add_column("Group ID", style="cyan", no_wrap=True)
    table.add_column("Name", style="green")
    table.add_column("Description", style="dim")
    table.add_column("Members", justify="right")
    table.add_column("Messages", justify="right")

    for group in groups:
        members = group.get("members")
        member_count = members.get("count") if members and isinstance(members, dict) else "?"
        messages = group.get("messages")
        message_count = messages.get("count") if messages and isinstance(messages, dict) else "?"

        table.add_row(
            group["id"],
            group.get("name", "Unknown")[:40],
            (group.get("description") or "")[:50],
            str(member_count),
            str(message_count),
        )

    console.print(table)
