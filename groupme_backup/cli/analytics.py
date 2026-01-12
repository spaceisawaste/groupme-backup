"""Analytics CLI commands."""

from datetime import datetime, timezone

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from ..analytics import queries
from ..db.session import get_session
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
@click.argument("group_identifier")
@click.option("--days", default=7, help="Number of days to analyze")
@click.option("--limit", default=10, help="Number of results to show")
@click.pass_context
def popular(ctx: click.Context, group_identifier: str, days: int, limit: int) -> None:
    """Show most popular messages by likes.

    GROUP_IDENTIFIER can be a numeric index (from 'groups' command) or group ID.

    Examples:
        groupme-backup popular 1 --days 7
        groupme-backup popular 13641782 --days 7
    """
    group_id = parse_group_identifier(group_identifier)

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
@click.argument("group_identifier")
@click.pass_context
def consecutive(ctx: click.Context, group_identifier: str) -> None:
    """Find longest consecutive message streak.

    GROUP_IDENTIFIER can be a numeric index (from 'groups' command) or group ID.

    Examples:
        groupme-backup consecutive 1
        groupme-backup consecutive 13641782
    """
    group_id = parse_group_identifier(group_identifier)

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
@click.argument("group_identifier")
@click.option("--days", default=30, help="Number of days to analyze")
@click.option("--limit", default=10, help="Number of users to show")
@click.pass_context
def active(ctx: click.Context, group_identifier: str, days: int, limit: int) -> None:
    """Show most active users by message count.

    GROUP_IDENTIFIER can be a numeric index (from 'groups' command) or group ID.

    Examples:
        groupme-backup active 1 --days 30
        groupme-backup active 13641782 --days 30
    """
    group_id = parse_group_identifier(group_identifier)

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
@click.argument("group_identifier")
@click.option("--days", default=30, help="Number of days to analyze")
@click.option("--limit", default=10, help="Number of users to show")
@click.pass_context
def liked(ctx: click.Context, group_identifier: str, days: int, limit: int) -> None:
    """Show most liked users (by total likes received).

    GROUP_IDENTIFIER can be a numeric index (from 'groups' command) or group ID.

    Examples:
        groupme-backup liked 1 --days 30
        groupme-backup liked 13641782 --days 30
    """
    group_id = parse_group_identifier(group_identifier)

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
@click.argument("group_identifier")
@click.pass_context
def stats(ctx: click.Context, group_identifier: str) -> None:
    """Show general statistics for a group.

    GROUP_IDENTIFIER can be a numeric index (from 'groups' command) or group ID.

    Examples:
        groupme-backup stats 1
        groupme-backup stats 13641782
    """
    group_id = parse_group_identifier(group_identifier)

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
@click.argument("group_identifier")
@click.pass_context
def response_time(ctx: click.Context, group_identifier: str) -> None:
    """Analyze conversation pace (time between messages).

    GROUP_IDENTIFIER can be a numeric index (from 'groups' command) or group ID.

    Examples:
        groupme-backup response-time 1
        groupme-backup response-time 13641782
    """
    group_id = parse_group_identifier(group_identifier)

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


@cli.command()
@click.argument("group_identifier")
@click.argument("text", required=False)
@click.option("--user", help="Filter by username")
@click.option("--liked-by", help="Filter by who liked the message")
@click.option("--after", help="Messages after date (YYYY-MM-DD)")
@click.option("--before", help="Messages before date (YYYY-MM-DD)")
@click.option("--case-sensitive", is_flag=True, help="Case-sensitive text search")
@click.option("--exact", is_flag=True, help="Exact phrase match")
@click.option("--limit", default=50, help="Maximum results to show")
@click.option("--with-context", is_flag=True, help="Show 3 messages before/after each result")
@click.pass_context
def search(
    ctx: click.Context,
    group_identifier: str,
    text: str,
    user: str,
    liked_by: str,
    after: str,
    before: str,
    case_sensitive: bool,
    exact: bool,
    limit: int,
    with_context: bool,
) -> None:
    """Search messages with flexible filtering.

    GROUP_IDENTIFIER can be a numeric index (from 'groups' command) or group ID.
    TEXT is an optional search term (case-insensitive by default).

    Examples:
        groupme-backup search 1 "pizza party"
        groupme-backup search 1 --user "John"
        groupme-backup search 1 "pizza" --user "John"
        groupme-backup search 1 --after 2024-01-01 --before 2024-12-31
        groupme-backup search 1 --liked-by "Sarah"
        groupme-backup search 1 "urgent" --case-sensitive --exact
        groupme-backup search 1 "meeting" --with-context
    """
    group_id = parse_group_identifier(group_identifier)

    # Parse dates
    after_date = None
    before_date = None
    if after:
        try:
            after_date = datetime.strptime(after, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            console.print(f"[red]Invalid date format for --after: {after}[/red]")
            console.print("Use format: YYYY-MM-DD (e.g., 2024-01-01)")
            raise click.Abort()

    if before:
        try:
            before_date = datetime.strptime(before, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            console.print(f"[red]Invalid date format for --before: {before}[/red]")
            console.print("Use format: YYYY-MM-DD (e.g., 2024-12-31)")
            raise click.Abort()

    # Validate at least one search criterion
    if not any([text, user, liked_by, after_date, before_date]):
        console.print("[red]Error:[/red] Please provide at least one search criterion")
        console.print("  - Search text (positional argument)")
        console.print("  - --user username")
        console.print("  - --liked-by username")
        console.print("  - --after date")
        console.print("  - --before date")
        raise click.Abort()

    with get_session() as session:
        results = queries.search_messages(
            session=session,
            group_id=group_id,
            text=text,
            user=user,
            liked_by=liked_by,
            after=after_date,
            before=before_date,
            case_sensitive=case_sensitive,
            exact=exact,
            limit=limit,
        )

        if not results:
            console.print("[yellow]No messages found matching your criteria[/yellow]")
            return

        # Build title with filters
        title_parts = []
        if text:
            title_parts.append(f'"{text}"')
        if user:
            title_parts.append(f"by {user}")
        if liked_by:
            title_parts.append(f"liked by {liked_by}")
        if after_date:
            title_parts.append(f"after {after}")
        if before_date:
            title_parts.append(f"before {before}")

        title = f"Search Results: {' '.join(title_parts)} ({len(results)} found)"
        console.print(f"\n[bold]{title}[/bold]\n")

        # Display results
        if with_context:
            # Show detailed view with context
            for i, msg in enumerate(results, 1):
                context = queries.get_message_context(session, group_id, msg["message_id"])

                # Build context display
                lines = []

                # Before messages (dimmed)
                if context.get("before"):
                    for before_msg in context["before"]:
                        lines.append(
                            f"[dim]{before_msg['created_at'].strftime('%Y-%m-%d %H:%M')} "
                            f"{before_msg['sender_name']}: {before_msg['text'][:80]}[/dim]"
                        )

                # Target message (highlighted)
                lines.append(
                    f"[bold green]{msg['created_at'].strftime('%Y-%m-%d %H:%M')} "
                    f"{msg['sender_name']}[/bold green]: [white]{msg['text'][:200]}[/white]"
                )
                if msg['like_count'] > 0:
                    lines.append(f"[magenta]❤ {msg['like_count']} likes[/magenta]")

                # After messages (dimmed)
                if context.get("after"):
                    for after_msg in context["after"]:
                        lines.append(
                            f"[dim]{after_msg['created_at'].strftime('%Y-%m-%d %H:%M')} "
                            f"{after_msg['sender_name']}: {after_msg['text'][:80]}[/dim]"
                        )

                panel = Panel(
                    "\n".join(lines),
                    title=f"Result {i}/{len(results)}",
                    border_style="blue",
                )
                console.print(panel)
                console.print()

        else:
            # Show table view
            table = Table(title=title)
            table.add_column("#", style="cyan", justify="right", width=4)
            table.add_column("Date", style="dim", width=16)
            table.add_column("Sender", style="green", width=20)
            table.add_column("Message", style="white")
            table.add_column("Likes", style="magenta", justify="right", width=6)

            for i, msg in enumerate(results, 1):
                text_preview = msg["text"][:80]
                if len(msg["text"]) > 80:
                    text_preview += "..."

                table.add_row(
                    str(i),
                    msg["created_at"].strftime("%Y-%m-%d %H:%M"),
                    msg["sender_name"][:20],
                    text_preview,
                    str(msg["like_count"]),
                )

            console.print(table)
            console.print()

            if len(results) == limit:
                console.print(
                    f"[dim]Showing first {limit} results. "
                    "Use --limit to see more.[/dim]"
                )


@cli.command()
@click.argument("group_identifier")
@click.option("--user", required=True, help="Username to search for")
@click.pass_context
def aliases(ctx: click.Context, group_identifier: str, user: str) -> None:
    """Show all names (aliases) a user has used.

    GROUP_IDENTIFIER can be a numeric index (from 'groups' command) or group ID.

    Examples:
        groupme-backup aliases 1 --user "Calm"
        groupme-backup aliases 1 --user "Chuck"
    """
    group_id = parse_group_identifier(group_identifier)

    with get_session() as session:
        result = queries.get_user_aliases(session, group_id, user)

        if "error" in result:
            console.print(f"[red]Error:[/red] {result['error']}")
            return

        console.print(f"\n[bold]{result['current_name']}[/bold]")
        console.print(f"User ID: [dim]{result['user_id']}[/dim]")
        console.print(f"Total aliases: [cyan]{result['total_aliases']}[/cyan]\n")

        table = Table(title=f"All Names Used ({result['total_aliases']} total)")
        table.add_column("#", style="dim", justify="right", width=4)
        table.add_column("Name", style="green")
        table.add_column("Messages", style="cyan", justify="right")
        table.add_column("First Used", style="dim")
        table.add_column("Last Used", style="dim")

        for i, alias in enumerate(result["aliases"], 1):
            # Highlight current name
            name_style = "bold green" if alias["name"] == result["current_name"] else "white"
            name = f"[{name_style}]{alias['name']}[/{name_style}]"
            if alias["name"] == result["current_name"]:
                name += " [cyan](current)[/cyan]"

            table.add_row(
                str(i),
                name,
                f"{alias['message_count']:,}",
                alias["first_used"].strftime("%Y-%m-%d"),
                alias["last_used"].strftime("%Y-%m-%d"),
            )

        console.print(table)
        console.print()


@cli.command("all-aliases")
@click.argument("group_identifier")
@click.option("--min", "min_aliases", default=2, help="Minimum number of aliases")
@click.option("--limit", default=20, help="Number of users to show")
@click.pass_context
def all_aliases(ctx: click.Context, group_identifier: str, min_aliases: int, limit: int) -> None:
    """Show all users with multiple names (aliases).

    GROUP_IDENTIFIER can be a numeric index (from 'groups' command) or group ID.

    Examples:
        groupme-backup all-aliases 1
        groupme-backup all-aliases 1 --min 5
    """
    group_id = parse_group_identifier(group_identifier)

    with get_session() as session:
        results = queries.get_all_users_with_aliases(session, group_id, min_aliases)

        if not results:
            console.print(f"[yellow]No users found with {min_aliases}+ aliases[/yellow]")
            return

        # Limit results
        results = results[:limit]

        table = Table(title=f"Users with Multiple Names ({len(results)} shown)")
        table.add_column("Rank", style="cyan", justify="right")
        table.add_column("Current Name", style="green")
        table.add_column("Aliases", style="magenta", justify="right")
        table.add_column("Messages", style="dim", justify="right")

        for i, user in enumerate(results, 1):
            table.add_row(
                str(i),
                user["current_name"][:40],
                str(user["alias_count"]),
                f"{user['total_messages']:,}",
            )

        console.print(table)
        console.print(f"\n[dim]Use 'groupme-backup aliases {group_identifier} --user <name>' to see details[/dim]")
        console.print()


@cli.command("by-name")
@click.argument("group_identifier")
@click.option("--user", required=True, help="Current username to search for")
@click.option("--name", "historical_name", required=True, help="Historical name/alias to filter by")
@click.option("--limit", default=50, help="Maximum messages to show")
@click.pass_context
def by_name(
    ctx: click.Context,
    group_identifier: str,
    user: str,
    historical_name: str,
    limit: int,
) -> None:
    """Show messages sent under a specific name.

    This shows messages from a particular "era" of a user's naming history.

    GROUP_IDENTIFIER can be a numeric index (from 'groups' command) or group ID.

    Examples:
        groupme-backup by-name 1 --user "Calm" --name "Certified Boy Lover"
        groupme-backup by-name 1 --user "Chickity" --name "The Goopster"
    """
    group_id = parse_group_identifier(group_identifier)

    with get_session() as session:
        result = queries.get_messages_by_name(
            session, group_id, user, historical_name, limit
        )

        if "error" in result:
            console.print(f"[red]Error:[/red] {result['error']}")
            return

        console.print(f"\n[bold]{result['current_name']}[/bold]")
        console.print(f"Historical name: [green]{result['historical_name']}[/green]")
        console.print(
            f"Period: {result['date_range']['first'].strftime('%Y-%m-%d')} to "
            f"{result['date_range']['last'].strftime('%Y-%m-%d')}"
        )
        console.print(f"Messages during this period: [cyan]{result['message_count']}[/cyan]\n")

        table = Table(title=f"Messages as '{result['historical_name']}'")
        table.add_column("#", style="dim", justify="right", width=4)
        table.add_column("Date", style="dim", width=16)
        table.add_column("Message", style="white")
        table.add_column("Likes", style="magenta", justify="right", width=6)

        for i, msg in enumerate(result["messages"], 1):
            text_preview = msg["text"][:80]
            if len(msg["text"]) > 80:
                text_preview += "..."

            table.add_row(
                str(i),
                msg["created_at"].strftime("%Y-%m-%d %H:%M"),
                text_preview,
                str(msg["like_count"]),
            )

        console.print(table)
        console.print()

        if result["message_count"] == limit:
            console.print(
                f"[dim]Showing first {limit} results. Use --limit to see more.[/dim]"
            )
