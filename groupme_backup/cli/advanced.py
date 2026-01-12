"""Advanced analytics CLI commands."""

import click
from rich.console import Console
from rich.table import Table

from ..analytics import queries
from ..db.session import get_session
from .main import cli

console = Console()


def parse_group_identifier(identifier: str) -> str:
    """Parse group identifier - can be a numeric index or group ID."""
    from ..utils.groups_cache import get_group_id_by_index

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
        return identifier


# ============================================================================
# TIME-BASED ANALYTICS
# ============================================================================

@cli.command()
@click.argument("group_identifier")
@click.pass_context
def peak_times(ctx: click.Context, group_identifier: str) -> None:
    """Show peak activity times (most active hour and day).

    Example: groupme-backup peak-times 1
    """
    group_id = parse_group_identifier(group_identifier)

    with get_session() as session:
        result = queries.get_peak_activity_times(session, group_id)

        if result:
            console.print("\n[bold]Peak Activity Time:[/bold]\n")
            console.print(f"[green]Day:[/green] {result['peak_day']}")
            console.print(f"[green]Hour:[/green] {result['peak_hour']}:00")
            console.print(f"[green]Messages:[/green] {result['message_count']}")
        else:
            console.print("[yellow]No activity data found[/yellow]")


@cli.command()
@click.argument("group_identifier")
@click.option("--days", default=30, help="Number of days to show")
@click.pass_context
def trend(ctx: click.Context, group_identifier: str, days: int) -> None:
    """Show daily message trend.

    Example: groupme-backup trend 1 --days 30
    """
    group_id = parse_group_identifier(group_identifier)

    with get_session() as session:
        results = queries.get_daily_message_trend(session, group_id, days)

        if results:
            table = Table(title=f"Daily Message Trend (Last {days} Days)")
            table.add_column("Date", style="cyan")
            table.add_column("Messages", justify="right", style="green")

            for row in results[-20:]:  # Show last 20 days
                table.add_row(
                    str(row["date"]),
                    str(row["message_count"])
                )

            console.print(table)
        else:
            console.print("[yellow]No trend data found[/yellow]")


# ============================================================================
# ATTACHMENT ANALYTICS
# ============================================================================

@cli.command()
@click.argument("group_identifier")
@click.option("--limit", default=10, help="Number of users to show")
@click.pass_context
def images(ctx: click.Context, group_identifier: str, limit: int) -> None:
    """Show users who share the most images.

    Example: groupme-backup images 1
    """
    group_id = parse_group_identifier(group_identifier)

    with get_session() as session:
        results = queries.get_image_sharing_stats(session, group_id, limit)

        if results:
            table = Table(title=f"Top {len(results)} Image Sharers")
            table.add_column("Rank", justify="right", style="cyan")
            table.add_column("Name", style="green")
            table.add_column("Images", justify="right", style="magenta")

            for i, row in enumerate(results, 1):
                table.add_row(
                    str(i),
                    row["name"][:30],
                    str(row["image_count"])
                )

            console.print(table)
        else:
            console.print("[yellow]No image data found[/yellow]")


@cli.command()
@click.argument("group_identifier")
@click.pass_context
def attachments(ctx: click.Context, group_identifier: str) -> None:
    """Show attachment type distribution.

    Example: groupme-backup attachments 1
    """
    group_id = parse_group_identifier(group_identifier)

    with get_session() as session:
        results = queries.get_attachment_type_distribution(session, group_id)

        if results:
            table = Table(title="Attachment Types")
            table.add_column("Type", style="cyan")
            table.add_column("Count", justify="right", style="green")

            for row in results:
                table.add_row(row["type"], str(row["count"]))

            console.print(table)
        else:
            console.print("[yellow]No attachment data found[/yellow]")


# ============================================================================
# ENGAGEMENT ANALYTICS
# ============================================================================

@cli.command()
@click.argument("group_identifier")
@click.option("--limit", default=10, help="Number of users to show")
@click.pass_context
def like_ratio(ctx: click.Context, group_identifier: str, limit: int) -> None:
    """Show users with best like-to-message ratio.

    Example: groupme-backup like-ratio 1
    """
    group_id = parse_group_identifier(group_identifier)

    with get_session() as session:
        results = queries.get_like_to_message_ratio(session, group_id, limit)

        if results:
            table = Table(title="Best Like-to-Message Ratios")
            table.add_column("Rank", justify="right", style="cyan")
            table.add_column("Name", style="green")
            table.add_column("Messages", justify="right")
            table.add_column("Total Likes", justify="right")
            table.add_column("Likes/Msg", justify="right", style="magenta")

            for i, row in enumerate(results, 1):
                table.add_row(
                    str(i),
                    row["name"][:30],
                    str(row["message_count"]),
                    str(row["total_likes"]),
                    f"{row['likes_per_message']:.2f}"
                )

            console.print(table)
        else:
            console.print("[yellow]No data found[/yellow]")


@cli.command()
@click.argument("group_identifier")
@click.option("--limit", default=10, help="Number of users to show")
@click.pass_context
def mentions(ctx: click.Context, group_identifier: str, limit: int) -> None:
    """Show most mentioned users.

    Example: groupme-backup mentions 1
    """
    group_id = parse_group_identifier(group_identifier)

    with get_session() as session:
        results = queries.get_most_mentioned_users(session, group_id, limit)

        if results:
            table = Table(title="Most Mentioned Users")
            table.add_column("Rank", justify="right", style="cyan")
            table.add_column("Name", style="green")
            table.add_column("Mentions", justify="right", style="magenta")

            for i, row in enumerate(results, 1):
                table.add_row(
                    str(i),
                    row["name"][:30],
                    str(row["mention_count"])
                )

            console.print(table)
        else:
            console.print("[yellow]No mention data found[/yellow]")


@cli.command()
@click.argument("group_identifier")
@click.option("--threshold", default=60, help="Silence threshold in minutes")
@click.option("--limit", default=10, help="Number of users to show")
@click.pass_context
def starters(ctx: click.Context, group_identifier: str, threshold: int, limit: int) -> None:
    """Show who starts conversations after silences.

    Example: groupme-backup starters 1 --threshold 60
    """
    group_id = parse_group_identifier(group_identifier)

    with get_session() as session:
        results = queries.get_conversation_starters(session, group_id, threshold, limit)

        if results:
            table = Table(title=f"Conversation Starters (>{threshold}min silence)")
            table.add_column("Rank", justify="right", style="cyan")
            table.add_column("Name", style="green")
            table.add_column("Starts", justify="right", style="magenta")

            for i, row in enumerate(results, 1):
                table.add_row(
                    str(i),
                    row["name"][:30],
                    str(row["conversation_starts"])
                )

            console.print(table)
        else:
            console.print("[yellow]No data found[/yellow]")


# ============================================================================
# CONTENT ANALYTICS
# ============================================================================

@cli.command()
@click.argument("group_identifier")
@click.option("--limit", default=10, help="Number of users to show")
@click.pass_context
def message_length(ctx: click.Context, group_identifier: str, limit: int) -> None:
    """Show average message length by user.

    Example: groupme-backup message-length 1
    """
    group_id = parse_group_identifier(group_identifier)

    with get_session() as session:
        results = queries.get_message_length_stats(session, group_id, limit)

        if results:
            table = Table(title="Message Length Statistics")
            table.add_column("Name", style="green")
            table.add_column("Messages", justify="right")
            table.add_column("Avg", justify="right", style="cyan")
            table.add_column("Max", justify="right")
            table.add_column("Min", justify="right")

            for row in results:
                table.add_row(
                    row["name"][:30],
                    str(row["message_count"]),
                    str(row["avg_length"]),
                    str(row["max_length"]),
                    str(row["min_length"])
                )

            console.print(table)
        else:
            console.print("[yellow]No data found[/yellow]")


@cli.command()
@click.argument("group_identifier")
@click.option("--limit", default=20, help="Number of emojis to show")
@click.pass_context
def emojis(ctx: click.Context, group_identifier: str, limit: int) -> None:
    """Show most used emojis.

    Example: groupme-backup emojis 1
    """
    group_id = parse_group_identifier(group_identifier)

    with get_session() as session:
        results = queries.get_emoji_usage(session, group_id, limit)

        if results:
            table = Table(title=f"Top {len(results)} Emojis")
            table.add_column("Rank", justify="right", style="cyan")
            table.add_column("Emoji", style="yellow")
            table.add_column("Count", justify="right", style="green")

            for i, row in enumerate(results, 1):
                table.add_row(
                    str(i),
                    row["emoji"],
                    str(row["count"])
                )

            console.print(table)
        else:
            console.print("[yellow]No emoji data found[/yellow]")


# ============================================================================
# USER-SPECIFIC TOOLS
# ============================================================================

@cli.command()
@click.argument("group_identifier")
@click.argument("user_id")
@click.pass_context
def aliases(ctx: click.Context, group_identifier: str, user_id: str) -> None:
    """Show all names (aliases) a user has used.

    Example: groupme-backup aliases 1 12345678
    """
    group_id = parse_group_identifier(group_identifier)

    with get_session() as session:
        results = queries.get_user_name_history(session, group_id, user_id)

        if results:
            table = Table(title=f"Name History for User {user_id}")
            table.add_column("Name", style="green")
            table.add_column("First Used", style="cyan")
            table.add_column("Last Used", style="cyan")
            table.add_column("Messages", justify="right", style="magenta")

            for row in results:
                table.add_row(
                    row["name"],
                    row["first_used"].strftime("%Y-%m-%d"),
                    row["last_used"].strftime("%Y-%m-%d"),
                    str(row["message_count"])
                )

            console.print(table)
        else:
            console.print(f"[yellow]No name history found for user {user_id}[/yellow]")


@cli.command()
@click.argument("group_identifier")
@click.argument("name")
@click.option("--limit", default=20, help="Number of messages to show")
@click.pass_context
def by_name(ctx: click.Context, group_identifier: str, name: str, limit: int) -> None:
    """Show messages sent under a specific name.

    Example: groupme-backup by-name 1 "John"
    """
    group_id = parse_group_identifier(group_identifier)

    with get_session() as session:
        results = queries.get_messages_by_name(session, group_id, name, limit)

        if results:
            table = Table(title=f"Messages from '{name}' (showing {len(results)})")
            table.add_column("Date", style="cyan")
            table.add_column("Name", style="green")
            table.add_column("Message", style="white")

            for row in results[:10]:  # Show first 10
                text = row["text"][:60]
                if len(row["text"]) > 60:
                    text += "..."

                table.add_row(
                    row["created_at"].strftime("%Y-%m-%d"),
                    row["name"][:20],
                    text
                )

            console.print(table)
            console.print(f"\n[dim]Found {len(results)} messages[/dim]")
        else:
            console.print(f"[yellow]No messages found for name '{name}'[/yellow]")


@cli.command()
@click.argument("group_identifier")
@click.pass_context
def all_aliases(ctx: click.Context, group_identifier: str) -> None:
    """Show all users with multiple names (aliases).

    Example: groupme-backup all-aliases 1
    """
    group_id = parse_group_identifier(group_identifier)

    with get_session() as session:
        results = queries.get_user_aliases(session, group_id)

        if results:
            table = Table(title="Users with Multiple Names")
            table.add_column("User ID", style="cyan")
            table.add_column("Aliases", justify="right", style="magenta")
            table.add_column("Names", style="green")

            for row in results[:20]:  # Show first 20
                names_str = ", ".join(row["names"][:5])
                if len(row["names"]) > 5:
                    names_str += f" (+{len(row['names']) - 5} more)"

                table.add_row(
                    row["user_id"],
                    str(row["alias_count"]),
                    names_str
                )

            console.print(table)
        else:
            console.print("[yellow]No users with multiple names found[/yellow]")


# ============================================================================
# SOCIAL NETWORK ANALYTICS
# ============================================================================

@cli.command()
@click.argument("group_identifier")
@click.option("--limit", default=20, help="Number of interactions to show")
@click.pass_context
def who_mentions_who(ctx: click.Context, group_identifier: str, limit: int) -> None:
    """Show who @mentions whom the most.

    Example: groupme-backup who-mentions-who 1
    """
    group_id = parse_group_identifier(group_identifier)

    with get_session() as session:
        results = queries.get_mention_interaction_matrix(session, group_id, limit)

        if results:
            table = Table(title="Mention Interactions")
            table.add_column("Mentioner", style="green")
            table.add_column("→", style="dim")
            table.add_column("Mentioned", style="cyan")
            table.add_column("Count", justify="right", style="magenta")

            for row in results:
                table.add_row(
                    row["mentioner_name"][:20],
                    "→",
                    row["mentioned_name"][:20],
                    str(row["mention_count"])
                )

            console.print(table)
        else:
            console.print("[yellow]No mention data found[/yellow]")


@cli.command()
@click.argument("group_identifier")
@click.option("--days", default=30, help="Number of days to analyze (default 30)")
@click.option("--window", default=5, help="Time window in minutes for replies")
@click.option("--limit", default=20, help="Number of patterns to show")
@click.pass_context
def reply_patterns(ctx: click.Context, group_identifier: str, days: int, window: int, limit: int) -> None:
    """Show who responds to whom most often.

    Analyzes recent messages to find reply patterns. Limited to recent days
    for performance on large datasets.

    Example: groupme-backup reply-patterns 1 --days 30 --window 5
    """
    group_id = parse_group_identifier(group_identifier)

    with get_session() as session:
        results = queries.get_reply_patterns(session, group_id, days, window, limit)

        if results:
            table = Table(title=f"Reply Patterns (Last {days} Days, within {window}min)")
            table.add_column("First User", style="green")
            table.add_column("→", style="dim")
            table.add_column("Responder", style="cyan")
            table.add_column("Replies", justify="right", style="magenta")
            table.add_column("Avg Time", justify="right")

            for row in results:
                table.add_row(
                    row["first_user_name"][:20],
                    "→",
                    row["second_user_name"][:20],
                    str(row["reply_count"]),
                    f"{row['avg_response_minutes']:.1f}m"
                )

            console.print(table)
        else:
            console.print("[yellow]No reply pattern data found[/yellow]")


# ============================================================================
# LEADERBOARDS
# ============================================================================

@cli.command()
@click.argument("group_identifier")
@click.option("--limit", default=10, help="Number of users to show")
@click.pass_context
def night_owl(ctx: click.Context, group_identifier: str, limit: int) -> None:
    """Show users most active between midnight and 5 AM.

    Example: groupme-backup night-owl 1
    """
    group_id = parse_group_identifier(group_identifier)

    with get_session() as session:
        results = queries.get_night_owl_leaderboard(session, group_id, limit)

        if results:
            table = Table(title="Night Owl Leaderboard (12 AM - 5 AM)")
            table.add_column("Rank", justify="right", style="cyan")
            table.add_column("Name", style="green")
            table.add_column("Night Messages", justify="right", style="magenta")
            table.add_column("%", justify="right")

            for i, row in enumerate(results, 1):
                table.add_row(
                    str(i),
                    row["name"][:30],
                    str(row["night_messages"]),
                    f"{row['percentage']:.1f}%"
                )

            console.print(table)
        else:
            console.print("[yellow]No night owl data found[/yellow]")


@cli.command()
@click.argument("group_identifier")
@click.option("--limit", default=10, help="Number of users to show")
@click.pass_context
def early_bird(ctx: click.Context, group_identifier: str, limit: int) -> None:
    """Show users most active between 5 AM and 9 AM.

    Example: groupme-backup early-bird 1
    """
    group_id = parse_group_identifier(group_identifier)

    with get_session() as session:
        results = queries.get_early_bird_leaderboard(session, group_id, limit)

        if results:
            table = Table(title="Early Bird Leaderboard (5 AM - 9 AM)")
            table.add_column("Rank", justify="right", style="cyan")
            table.add_column("Name", style="green")
            table.add_column("Morning Messages", justify="right", style="magenta")
            table.add_column("%", justify="right")

            for i, row in enumerate(results, 1):
                table.add_row(
                    str(i),
                    row["name"][:30],
                    str(row["morning_messages"]),
                    f"{row['percentage']:.1f}%"
                )

            console.print(table)
        else:
            console.print("[yellow]No early bird data found[/yellow]")


@cli.command()
@click.argument("group_identifier")
@click.option("--limit", default=10, help="Number of users to show")
@click.pass_context
def weekend_warrior(ctx: click.Context, group_identifier: str, limit: int) -> None:
    """Show users most active on weekends.

    Example: groupme-backup weekend-warrior 1
    """
    group_id = parse_group_identifier(group_identifier)

    with get_session() as session:
        results = queries.get_weekend_warrior_leaderboard(session, group_id, limit)

        if results:
            table = Table(title="Weekend Warrior Leaderboard")
            table.add_column("Rank", justify="right", style="cyan")
            table.add_column("Name", style="green")
            table.add_column("Weekend Msgs", justify="right")
            table.add_column("Total", justify="right")
            table.add_column("Weekend %", justify="right", style="magenta")

            for i, row in enumerate(results, 1):
                table.add_row(
                    str(i),
                    row["name"][:30],
                    str(row["weekend_messages"]),
                    str(row["total_messages"]),
                    f"{row['weekend_percentage']:.1f}%"
                )

            console.print(table)
        else:
            console.print("[yellow]No weekend warrior data found[/yellow]")


@cli.command()
@click.argument("group_identifier")
@click.option("--days", default=30, help="Number of days to analyze")
@click.option("--limit", default=10, help="Number of messages to show")
@click.pass_context
def controversial(ctx: click.Context, group_identifier: str, days: int, limit: int) -> None:
    """Show controversial messages (high likes AND many replies).

    Example: groupme-backup controversial 1 --days 30
    """
    group_id = parse_group_identifier(group_identifier)

    with get_session() as session:
        results = queries.get_controversial_messages(session, group_id, days, limit)

        if results:
            table = Table(title=f"Controversial Messages (Last {days} Days)")
            table.add_column("Date", style="cyan")
            table.add_column("Name", style="green")
            table.add_column("Message", style="white")
            table.add_column("Likes", justify="right")
            table.add_column("Replies", justify="right")
            table.add_column("Score", justify="right", style="magenta")

            for row in results:
                text = row["text"][:40]
                if len(row["text"]) > 40:
                    text += "..."

                table.add_row(
                    row["created_at"].strftime("%m/%d"),
                    row["name"][:15],
                    text,
                    str(row["like_count"]),
                    str(row["reply_count"]),
                    str(row["controversy_score"])
                )

            console.print(table)
        else:
            console.print("[yellow]No controversial messages found[/yellow]")
