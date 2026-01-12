"""Analytics CLI commands."""

import click
from rich.console import Console
from rich.table import Table

from ..analytics import queries
from ..db.session import get_session
from .main import cli

console = Console()


@cli.command()
@click.argument("group_id")
@click.option("--days", default=7, help="Number of days to analyze")
@click.option("--limit", default=10, help="Number of results to show")
@click.pass_context
def popular(ctx: click.Context, group_id: str, days: int, limit: int) -> None:
    """Show most popular messages by likes.

    GROUP_ID is the ID of the group to analyze.

    Example: groupme-backup popular 12345678 --days 7
    """
    with get_session() as session:
        results = queries.get_most_popular_messages(session, group_id, days, limit)

        if not results:
            console.print("[yellow]No messages found for this time period[/yellow]")
            return

        table = Table(
            title=f"Top {len(results)} Most Liked Messages (Last {days} Days)"
        )
        table.add_column("#", style="cyan", justify="right")
        table.add_column("Likes", style="magenta", justify="right")
        table.add_column("Sender", style="green")
        table.add_column("Message", style="white")
        table.add_column("Date", style="dim")

        for i, msg in enumerate(results, 1):
            text = msg["text"][:60]
            if len(msg["text"]) > 60:
                text += "..."

            table.add_row(
                str(i),
                str(msg["like_count"]),
                msg["sender_name"][:20],
                text,
                msg["created_at"].strftime("%Y-%m-%d %H:%M"),
            )

        console.print(table)


@cli.command()
@click.argument("group_id")
@click.pass_context
def consecutive(ctx: click.Context, group_id: str) -> None:
    """Find longest consecutive message streak.

    GROUP_ID is the ID of the group to analyze.

    Example: groupme-backup consecutive 12345678
    """
    with get_session() as session:
        result = queries.get_longest_consecutive_streak(session, group_id)

        if not result:
            console.print("[yellow]No consecutive messages found[/yellow]")
            return

        console.print(
            f"\n[bold]Longest Consecutive Message Streak:[/bold]\n"
        )
        console.print(f"[green]User:[/green] {result['name']}")
        console.print(f"[green]Messages:[/green] {result['consecutive_count']}")
        console.print(
            f"[green]Period:[/green] {result['streak_start'].strftime('%Y-%m-%d %H:%M')} "
            f"to {result['streak_end'].strftime('%Y-%m-%d %H:%M')}"
        )

        duration = result["streak_end"] - result["streak_start"]
        console.print(f"[green]Duration:[/green] {duration}")
        console.print()


@cli.command()
@click.argument("group_id")
@click.option("--days", default=30, help="Number of days to analyze")
@click.option("--limit", default=10, help="Number of users to show")
@click.pass_context
def active(ctx: click.Context, group_id: str, days: int, limit: int) -> None:
    """Show most active users by message count.

    GROUP_ID is the ID of the group to analyze.

    Example: groupme-backup active 12345678 --days 30
    """
    with get_session() as session:
        results = queries.get_most_active_users(session, group_id, days, limit)

        if not results:
            console.print("[yellow]No users found for this time period[/yellow]")
            return

        table = Table(title=f"Top {len(results)} Most Active Users (Last {days} Days)")
        table.add_column("Rank", style="cyan", justify="right")
        table.add_column("User", style="green")
        table.add_column("Messages", style="magenta", justify="right")

        for i, user in enumerate(results, 1):
            table.add_row(
                str(i), user["name"][:30], str(user["message_count"])
            )

        console.print(table)


@cli.command()
@click.argument("group_id")
@click.option("--days", default=30, help="Number of days to analyze")
@click.option("--limit", default=10, help="Number of users to show")
@click.pass_context
def liked(ctx: click.Context, group_id: str, days: int, limit: int) -> None:
    """Show most liked users (by total likes received).

    GROUP_ID is the ID of the group to analyze.

    Example: groupme-backup liked 12345678 --days 30
    """
    with get_session() as session:
        results = queries.get_most_liked_users(session, group_id, days, limit)

        if not results:
            console.print("[yellow]No users found for this time period[/yellow]")
            return

        table = Table(title=f"Top {len(results)} Most Liked Users (Last {days} Days)")
        table.add_column("Rank", style="cyan", justify="right")
        table.add_column("User", style="green")
        table.add_column("Total Likes", style="magenta", justify="right")

        for i, user in enumerate(results, 1):
            table.add_row(str(i), user["name"][:30], str(user["total_likes"]))

        console.print(table)


@cli.command()
@click.argument("group_id")
@click.pass_context
def stats(ctx: click.Context, group_id: str) -> None:
    """Show general statistics for a group.

    GROUP_ID is the ID of the group to analyze.

    Example: groupme-backup stats 12345678
    """
    with get_session() as session:
        result = queries.get_group_statistics(session, group_id)

        if "error" in result:
            console.print(f"[red]Error:[/red] {result['error']}")
            return

        console.print(f"\n[bold]{result['group_name']}[/bold] Statistics\n")

        table = Table(show_header=False, box=None)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")

        table.add_row("Total Messages", f"{result['total_messages']:,}")
        table.add_row("Total Users", f"{result['total_users']:,}")
        table.add_row("Total Likes", f"{result['total_likes']:,}")
        table.add_row(
            "Avg Messages/Day", f"{result['avg_messages_per_day']:.2f}"
        )

        if result["first_message"]:
            table.add_row(
                "First Message",
                result["first_message"].strftime("%Y-%m-%d %H:%M"),
            )
        if result["last_message"]:
            table.add_row(
                "Last Message",
                result["last_message"].strftime("%Y-%m-%d %H:%M"),
            )
        if result["last_synced_at"]:
            table.add_row(
                "Last Synced",
                result["last_synced_at"].strftime("%Y-%m-%d %H:%M"),
            )

        console.print(table)
        console.print()


@cli.command()
@click.argument("group_id")
@click.pass_context
def response_time(ctx: click.Context, group_id: str) -> None:
    """Analyze conversation pace (time between messages).

    GROUP_ID is the ID of the group to analyze.

    Example: groupme-backup response-time 12345678
    """
    with get_session() as session:
        result = queries.get_response_time_analysis(session, group_id)

        console.print("\n[bold]Conversation Pace Analysis[/bold]\n")

        table = Table(show_header=False, box=None)
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")

        table.add_row(
            "Average Gap", f"{result['avg_gap_minutes']:.2f} minutes"
        )
        table.add_row(
            "Median Gap", f"{result['median_gap_minutes']:.2f} minutes"
        )
        table.add_row(
            "Minimum Gap", f"{result['min_gap_seconds']:.2f} seconds"
        )
        table.add_row(
            "Maximum Gap",
            f"{result['max_gap_seconds'] / 3600:.2f} hours",
        )

        console.print(table)
        console.print()
