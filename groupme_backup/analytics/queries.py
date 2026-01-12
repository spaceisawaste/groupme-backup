"""Analytics query functions for GroupMe data."""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from sqlalchemy import desc, func, text
from sqlalchemy.orm import Session

from ..db.models import Group, Message, MessageFavorite, User


def get_most_popular_messages(
    session: Session, group_id: str, days: int = 7, limit: int = 10
) -> List[Dict[str, Any]]:
    """
    Get messages with the most likes in a time period.

    Args:
        session: Database session
        group_id: Group ID to analyze
        days: Number of days to look back
        limit: Maximum number of results

    Returns:
        List of dictionaries with message info and like counts
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    query = (
        session.query(
            Message.id,
            Message.text,
            Message.name.label("sender_name"),
            Message.created_at,
            func.count(MessageFavorite.user_id).label("like_count"),
        )
        .outerjoin(MessageFavorite, Message.id == MessageFavorite.message_id)
        .filter(Message.group_id == group_id)
        .filter(Message.created_at >= cutoff)
        .filter(Message.system == False)
        .group_by(Message.id)
        .order_by(desc("like_count"))
        .limit(limit)
    )

    results = []
    for row in query.all():
        results.append(
            {
                "message_id": row.id,
                "text": row.text or "(no text)",
                "sender_name": row.sender_name or "Unknown",
                "created_at": row.created_at,
                "like_count": row.like_count,
            }
        )

    return results


def get_longest_consecutive_streak(
    session: Session, group_id: str
) -> Dict[str, Any] | None:
    """
    Find the longest consecutive message streak by a single user.

    Uses window functions to identify message runs.

    Args:
        session: Database session
        group_id: Group ID to analyze

    Returns:
        Dictionary with streak information or None
    """
    sql = text(
        """
    WITH message_groups AS (
        SELECT
            id,
            user_id,
            name,
            created_at,
            CASE
                WHEN user_id != LAG(user_id) OVER (ORDER BY created_at)
                    OR LAG(user_id) OVER (ORDER BY created_at) IS NULL
                THEN 1
                ELSE 0
            END AS is_new_group
        FROM messages
        WHERE group_id = :group_id
        AND system = FALSE
        ORDER BY created_at
    ),
    streak_groups AS (
        SELECT
            id,
            user_id,
            name,
            created_at,
            SUM(is_new_group) OVER (ORDER BY created_at) AS streak_id
        FROM message_groups
    ),
    streak_counts AS (
        SELECT
            user_id,
            name,
            streak_id,
            COUNT(*) AS consecutive_count,
            MIN(created_at) AS streak_start,
            MAX(created_at) AS streak_end
        FROM streak_groups
        GROUP BY user_id, name, streak_id
    )
    SELECT
        user_id,
        name,
        consecutive_count,
        streak_start,
        streak_end
    FROM streak_counts
    ORDER BY consecutive_count DESC
    LIMIT 1;
    """
    )

    result = session.execute(sql, {"group_id": group_id}).fetchone()

    if result:
        return {
            "user_id": result[0],
            "name": result[1],
            "consecutive_count": result[2],
            "streak_start": result[3],
            "streak_end": result[4],
        }

    return None


def get_most_active_users(
    session: Session, group_id: str, days: int = 30, limit: int = 10
) -> List[Dict[str, Any]]:
    """
    Get users with the most messages in a time period.

    Args:
        session: Database session
        group_id: Group ID to analyze
        days: Number of days to look back
        limit: Maximum number of results

    Returns:
        List of dictionaries with user info and message counts
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    query = (
        session.query(
            User.id,
            User.name,
            func.count(Message.id).label("message_count"),
        )
        .join(Message, User.id == Message.user_id)
        .filter(Message.group_id == group_id)
        .filter(Message.created_at >= cutoff)
        .filter(Message.system == False)
        .group_by(User.id, User.name)
        .order_by(desc("message_count"))
        .limit(limit)
    )

    results = []
    for row in query.all():
        results.append(
            {
                "user_id": row.id,
                "name": row.name or "Unknown",
                "message_count": row.message_count,
            }
        )

    return results


def get_most_liked_users(
    session: Session, group_id: str, days: int = 30, limit: int = 10
) -> List[Dict[str, Any]]:
    """
    Get users whose messages receive the most likes.

    Args:
        session: Database session
        group_id: Group ID to analyze
        days: Number of days to look back
        limit: Maximum number of results

    Returns:
        List of dictionaries with user info and total likes
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    query = (
        session.query(
            User.id,
            User.name,
            func.count(MessageFavorite.user_id).label("total_likes"),
        )
        .join(Message, User.id == Message.user_id)
        .join(MessageFavorite, Message.id == MessageFavorite.message_id)
        .filter(Message.group_id == group_id)
        .filter(Message.created_at >= cutoff)
        .filter(Message.system == False)
        .group_by(User.id, User.name)
        .order_by(desc("total_likes"))
        .limit(limit)
    )

    results = []
    for row in query.all():
        results.append(
            {
                "user_id": row.id,
                "name": row.name or "Unknown",
                "total_likes": row.total_likes,
            }
        )

    return results


def get_group_statistics(session: Session, group_id: str) -> Dict[str, Any]:
    """
    Get general statistics for a group.

    Args:
        session: Database session
        group_id: Group ID to analyze

    Returns:
        Dictionary with various statistics
    """
    # Get group info
    group = session.query(Group).filter(Group.id == group_id).first()

    if not group:
        return {"error": "Group not found"}

    # Total messages
    total_messages = (
        session.query(func.count(Message.id))
        .filter(Message.group_id == group_id)
        .filter(Message.system == False)
        .scalar()
    )

    # Total users
    total_users = (
        session.query(func.count(func.distinct(Message.user_id)))
        .filter(Message.group_id == group_id)
        .filter(Message.system == False)
        .scalar()
    )

    # Total likes
    total_likes = (
        session.query(func.count(MessageFavorite.user_id))
        .join(Message, MessageFavorite.message_id == Message.id)
        .filter(Message.group_id == group_id)
        .scalar()
    )

    # Date range
    date_range = (
        session.query(
            func.min(Message.created_at).label("first_message"),
            func.max(Message.created_at).label("last_message"),
        )
        .filter(Message.group_id == group_id)
        .filter(Message.system == False)
        .first()
    )

    # Average messages per day
    if date_range and date_range.first_message and date_range.last_message:
        days_span = (date_range.last_message - date_range.first_message).days + 1
        avg_messages_per_day = total_messages / days_span if days_span > 0 else 0
    else:
        avg_messages_per_day = 0

    return {
        "group_name": group.name,
        "total_messages": total_messages,
        "total_users": total_users,
        "total_likes": total_likes,
        "first_message": date_range.first_message if date_range else None,
        "last_message": date_range.last_message if date_range else None,
        "avg_messages_per_day": round(avg_messages_per_day, 2),
        "last_synced_at": group.last_synced_at,
    }


def get_hourly_activity_heatmap(
    session: Session, group_id: str
) -> List[Dict[str, Any]]:
    """
    Get message count by hour of day and day of week.

    Args:
        session: Database session
        group_id: Group ID to analyze

    Returns:
        List of dictionaries with day, hour, and message count
    """
    sql = text(
        """
    SELECT
        EXTRACT(DOW FROM created_at)::INTEGER AS day_of_week,
        EXTRACT(HOUR FROM created_at)::INTEGER AS hour_of_day,
        COUNT(*) AS message_count
    FROM messages
    WHERE group_id = :group_id
    AND system = FALSE
    GROUP BY day_of_week, hour_of_day
    ORDER BY day_of_week, hour_of_day;
    """
    )

    results = []
    for row in session.execute(sql, {"group_id": group_id}):
        results.append(
            {
                "day_of_week": row[0],  # 0=Sunday, 6=Saturday
                "hour_of_day": row[1],  # 0-23
                "message_count": row[2],
            }
        )

    return results


def get_response_time_analysis(session: Session, group_id: str) -> Dict[str, Any]:
    """
    Analyze average time between messages (conversation pace).

    Args:
        session: Database session
        group_id: Group ID to analyze

    Returns:
        Dictionary with response time statistics
    """
    sql = text(
        """
    WITH message_gaps AS (
        SELECT
            id,
            created_at,
            LAG(created_at) OVER (ORDER BY created_at) AS prev_message_time,
            EXTRACT(EPOCH FROM (
                created_at - LAG(created_at) OVER (ORDER BY created_at)
            )) AS gap_seconds
        FROM messages
        WHERE group_id = :group_id
        AND system = FALSE
    )
    SELECT
        AVG(gap_seconds) AS avg_gap_seconds,
        MIN(gap_seconds) AS min_gap_seconds,
        MAX(gap_seconds) AS max_gap_seconds,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY gap_seconds) AS median_gap_seconds
    FROM message_gaps
    WHERE gap_seconds IS NOT NULL
    AND gap_seconds > 0;
    """
    )

    result = session.execute(sql, {"group_id": group_id}).fetchone()

    if result and result[0] is not None:
        return {
            "avg_gap_seconds": float(result[0]),
            "min_gap_seconds": float(result[1]),
            "max_gap_seconds": float(result[2]),
            "median_gap_seconds": float(result[3]),
            "avg_gap_minutes": float(result[0]) / 60,
            "median_gap_minutes": float(result[3]) / 60,
        }

    return {
        "avg_gap_seconds": 0,
        "min_gap_seconds": 0,
        "max_gap_seconds": 0,
        "median_gap_seconds": 0,
        "avg_gap_minutes": 0,
        "median_gap_minutes": 0,
    }
