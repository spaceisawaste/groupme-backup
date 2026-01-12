"""Groups management CLI commands."""

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from ..api.client import GroupMeClient
from ..utils.groups_cache import save_groups_cache, load_groups_cache
from .main import cli

console = Console()


def parse_group_identifier(identifier: str) -> str:
    """
    Parse group identifier - can be a numeric index or group ID.

    Returns the group ID.
    """
    from ..utils.groups_cache import get_group_id_by_index

    # Try to parse as integer (numeric index)
    try:
        index = int(identifier)
        group_id = get_group_id_by_index(index)
        if group_id:
            return group_id
        else:
            console.print(f"[red]Error:[/red] No group at index {index}")
            console.print("Run [cyan]groupme-backup groups[/cyan] to see available groups")
            raise click.Abort()
    except ValueError:
        # Not a number, assume it's a group ID
        return identifier


@cli.command()
@click.option("--all", "show_all", is_flag=True, help="Show all groups (not just top 5)")
@click.option("--refresh", is_flag=True, help="Refresh groups from API")
@click.pass_context
def groups(ctx: click.Context, show_all: bool, refresh: bool) -> None:
    """List groups with numeric indices for easy reference.

    By default shows top 5 groups by recent activity.
    Use --all to show all groups.
    Use --refresh to fetch latest from API.
    """
    settings = ctx.obj["settings"]

    # Load from cache unless refresh requested
    if refresh or not load_groups_cache():
        console.print("[bold blue]Fetching groups from API...[/bold blue]")
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
            groups_list = api_client.get_all_groups()
            progress.update(task, completed=True)

        # Sort by last message time (most recent first)
        groups_list.sort(
            key=lambda g: g.get("messages", {}).get("last_message_created_at", 0)
            if isinstance(g.get("messages"), dict) else 0,
            reverse=True
        )

        save_groups_cache(groups_list)
        console.print("[green]Groups cache updated![/green]\n")
    else:
        groups_list = load_groups_cache()

    # Determine how many to show
    display_count = len(groups_list) if show_all else min(5, len(groups_list))
    groups_to_show = groups_list[:display_count]

    # Create table
    title = f"Your Groups ({display_count} of {len(groups_list)} shown)"
    if show_all:
        title = f"All Your Groups ({len(groups_list)} total)"

    table = Table(title=title)
    table.add_column("#", style="cyan", justify="right")
    table.add_column("Name", style="green")
    table.add_column("Messages", style="magenta", justify="right")
    table.add_column("Group ID", style="dim")

    for i, group in enumerate(groups_to_show, 1):
        messages = group.get("messages")
        if isinstance(messages, dict):
            message_count = messages.get("count", "?")
        else:
            message_count = "?"

        table.add_row(
            str(i),
            group.get("name", "Unknown")[:40],
            str(message_count),
            group["id"]
        )

    console.print(table)

    if not show_all and len(groups_list) > 5:
        console.print(
            f"\n[dim]Showing top 5 of {len(groups_list)} groups. "
            "Use [cyan]--all[/cyan] to see all groups.[/dim]"
        )

    console.print(
        "\n[dim]Use the [cyan]#[/cyan] number in analytics commands:[/dim]"
    )
    console.print("[dim]  Example: [cyan]groupme-backup popular 1[/cyan][/dim]")
