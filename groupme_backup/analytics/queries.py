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


# ============================================================================
# TIME-BASED ANALYTICS
# ============================================================================

def get_peak_activity_times(session: Session, group_id: str) -> Dict[str, Any]:
    """Get peak activity times (most active hour and day)."""
    sql = text("""
    SELECT 
        EXTRACT(DOW FROM created_at)::INTEGER AS day_of_week,
        EXTRACT(HOUR FROM created_at)::INTEGER AS hour_of_day,
        COUNT(*) AS message_count
    FROM messages
    WHERE group_id = :group_id AND system = FALSE
    GROUP BY day_of_week, hour_of_day
    ORDER BY message_count DESC
    LIMIT 1;
    """)
    
    result = session.execute(sql, {"group_id": group_id}).fetchone()
    
    if result:
        days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
        return {
            "peak_day": days[result[0]],
            "peak_hour": result[1],
            "message_count": result[2],
        }
    return {}


def get_daily_message_trend(
    session: Session, group_id: str, days: int = 30
) -> List[Dict[str, Any]]:
    """Get daily message counts for trend analysis."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    
    sql = text("""
    SELECT 
        DATE(created_at) AS message_date,
        COUNT(*) AS message_count
    FROM messages
    WHERE group_id = :group_id 
      AND system = FALSE
      AND created_at >= :cutoff
    GROUP BY message_date
    ORDER BY message_date;
    """)
    
    results = []
    for row in session.execute(sql, {"group_id": group_id, "cutoff": cutoff}):
        results.append({
            "date": row[0],
            "message_count": row[1],
        })
    
    return results


# ============================================================================
# ATTACHMENT ANALYTICS
# ============================================================================

def get_image_sharing_stats(
    session: Session, group_id: str, limit: int = 10
) -> List[Dict[str, Any]]:
    """Get users who share the most images."""
    from ..db.models import Attachment
    
    query = (
        session.query(
            User.id,
            User.name,
            func.count(Attachment.id).label("image_count"),
        )
        .join(Message, User.id == Message.user_id)
        .join(Attachment, Message.id == Attachment.message_id)
        .filter(Message.group_id == group_id)
        .filter(Attachment.type == "image")
        .group_by(User.id, User.name)
        .order_by(desc("image_count"))
        .limit(limit)
    )
    
    results = []
    for row in query.all():
        results.append({
            "user_id": row.id,
            "name": row.name or "Unknown",
            "image_count": row.image_count,
        })
    
    return results


def get_attachment_type_distribution(
    session: Session, group_id: str
) -> List[Dict[str, Any]]:
    """Get distribution of attachment types in the group."""
    from ..db.models import Attachment
    
    query = (
        session.query(
            Attachment.type,
            func.count(Attachment.id).label("count"),
        )
        .join(Message, Attachment.message_id == Message.id)
        .filter(Message.group_id == group_id)
        .group_by(Attachment.type)
        .order_by(desc("count"))
    )
    
    results = []
    for row in query.all():
        results.append({
            "type": row.type,
            "count": row.count,
        })
    
    return results


# ============================================================================
# ENGAGEMENT ANALYTICS
# ============================================================================

def get_like_to_message_ratio(
    session: Session, group_id: str, limit: int = 10
) -> List[Dict[str, Any]]:
    """Get users with best like-to-message ratio."""
    sql = text("""
    WITH user_stats AS (
        SELECT 
            m.user_id,
            m.name,
            COUNT(DISTINCT m.id) AS message_count,
            COUNT(DISTINCT mf.message_id) AS messages_with_likes,
            COUNT(mf.user_id) AS total_likes
        FROM messages m
        LEFT JOIN message_favorites mf ON m.id = mf.message_id
        WHERE m.group_id = :group_id AND m.system = FALSE
        GROUP BY m.user_id, m.name
        HAVING COUNT(DISTINCT m.id) >= 10  -- At least 10 messages
    )
    SELECT 
        user_id,
        name,
        message_count,
        total_likes,
        ROUND(CAST(total_likes AS NUMERIC) / message_count, 2) AS likes_per_message
    FROM user_stats
    ORDER BY likes_per_message DESC
    LIMIT :limit;
    """)
    
    results = []
    for row in session.execute(sql, {"group_id": group_id, "limit": limit}):
        results.append({
            "user_id": row[0],
            "name": row[1] or "Unknown",
            "message_count": row[2],
            "total_likes": row[3],
            "likes_per_message": float(row[4]),
        })
    
    return results


def get_most_mentioned_users(
    session: Session, group_id: str, limit: int = 10
) -> List[Dict[str, Any]]:
    """Get users who are @mentioned most often."""
    from ..db.models import Mention
    
    query = (
        session.query(
            User.id,
            User.name,
            func.count(Mention.id).label("mention_count"),
        )
        .join(Mention, User.id == Mention.user_id)
        .join(Message, Mention.message_id == Message.id)
        .filter(Message.group_id == group_id)
        .group_by(User.id, User.name)
        .order_by(desc("mention_count"))
        .limit(limit)
    )
    
    results = []
    for row in query.all():
        results.append({
            "user_id": row.id,
            "name": row.name or "Unknown",
            "mention_count": row.mention_count,
        })
    
    return results


def get_conversation_starters(
    session: Session, group_id: str, silence_threshold: int = 60, limit: int = 10
) -> List[Dict[str, Any]]:
    """Find who starts conversations after long silences (in minutes)."""
    sql = text("""
    WITH message_gaps AS (
        SELECT 
            id,
            user_id,
            name,
            created_at,
            EXTRACT(EPOCH FROM (created_at - LAG(created_at) OVER (ORDER BY created_at))) / 60 AS gap_minutes
        FROM messages
        WHERE group_id = :group_id AND system = FALSE
    )
    SELECT 
        user_id,
        name,
        COUNT(*) AS conversation_starts
    FROM message_gaps
    WHERE gap_minutes > :silence_threshold OR gap_minutes IS NULL
    GROUP BY user_id, name
    ORDER BY conversation_starts DESC
    LIMIT :limit;
    """)
    
    results = []
    for row in session.execute(sql, {
        "group_id": group_id,
        "silence_threshold": silence_threshold,
        "limit": limit
    }):
        results.append({
            "user_id": row[0],
            "name": row[1] or "Unknown",
            "conversation_starts": row[2],
        })
    
    return results


# ============================================================================
# CONTENT ANALYTICS
# ============================================================================

def get_message_length_stats(
    session: Session, group_id: str, limit: int = 10
) -> List[Dict[str, Any]]:
    """Get average message length by user."""
    sql = text("""
    SELECT 
        user_id,
        name,
        COUNT(*) AS message_count,
        ROUND(AVG(LENGTH(text))) AS avg_length,
        MAX(LENGTH(text)) AS max_length,
        MIN(LENGTH(text)) AS min_length
    FROM messages
    WHERE group_id = :group_id 
      AND system = FALSE 
      AND text IS NOT NULL
      AND text != ''
    GROUP BY user_id, name
    HAVING COUNT(*) >= 10
    ORDER BY avg_length DESC
    LIMIT :limit;
    """)
    
    results = []
    for row in session.execute(sql, {"group_id": group_id, "limit": limit}):
        results.append({
            "user_id": row[0],
            "name": row[1] or "Unknown",
            "message_count": row[2],
            "avg_length": int(row[3]) if row[3] else 0,
            "max_length": row[4],
            "min_length": row[5],
        })
    
    return results


def get_emoji_usage(
    session: Session, group_id: str, limit: int = 20
) -> List[Dict[str, Any]]:
    """Get emoji usage statistics from emoji attachments."""
    from ..db.models import Attachment
    
    sql = text("""
    SELECT 
        a.placeholder,
        COUNT(*) AS usage_count
    FROM attachments a
    JOIN messages m ON a.message_id = m.id
    WHERE m.group_id = :group_id 
      AND a.type = 'emoji'
      AND a.placeholder IS NOT NULL
    GROUP BY a.placeholder
    ORDER BY usage_count DESC
    LIMIT :limit;
    """)
    
    results = []
    for row in session.execute(sql, {"group_id": group_id, "limit": limit}):
        results.append({
            "emoji": row[0],
            "count": row[1],
        })
    
    return results


# ============================================================================
# USER-SPECIFIC ANALYTICS
# ============================================================================

def get_user_name_history(
    session: Session, group_id: str, user_id: str
) -> List[Dict[str, Any]]:
    """Get all names a user has used, with first/last seen dates."""
    sql = text("""
    SELECT DISTINCT
        name,
        MIN(created_at) AS first_used,
        MAX(created_at) AS last_used,
        COUNT(*) AS message_count
    FROM messages
    WHERE group_id = :group_id AND user_id = :user_id AND name IS NOT NULL
    GROUP BY name
    ORDER BY first_used;
    """)
    
    results = []
    for row in session.execute(sql, {"group_id": group_id, "user_id": user_id}):
        results.append({
            "name": row[0],
            "first_used": row[1],
            "last_used": row[2],
            "message_count": row[3],
        })
    
    return results


def get_messages_by_name(
    session: Session, group_id: str, name: str, limit: int = 50
) -> List[Dict[str, Any]]:
    """Get recent messages sent under a specific name."""
    query = (
        session.query(
            Message.id,
            Message.text,
            Message.name,
            Message.user_id,
            Message.created_at,
        )
        .filter(Message.group_id == group_id)
        .filter(Message.name.ilike(f"%{name}%"))
        .filter(Message.system == False)
        .order_by(desc(Message.created_at))
        .limit(limit)
    )
    
    results = []
    for row in query.all():
        results.append({
            "message_id": row.id,
            "text": row.text or "(no text)",
            "name": row.name,
            "user_id": row.user_id,
            "created_at": row.created_at,
        })
    
    return results


def get_user_aliases(session: Session, group_id: str) -> List[Dict[str, Any]]:
    """Get users with multiple names (aliases)."""
    sql = text("""
    WITH user_names AS (
        SELECT 
            user_id,
            ARRAY_AGG(DISTINCT name ORDER BY name) AS names,
            COUNT(DISTINCT name) AS name_count
        FROM messages
        WHERE group_id = :group_id AND name IS NOT NULL AND user_id IS NOT NULL
        GROUP BY user_id
        HAVING COUNT(DISTINCT name) > 1
    )
    SELECT 
        user_id,
        names,
        name_count
    FROM user_names
    ORDER BY name_count DESC;
    """)
    
    results = []
    for row in session.execute(sql, {"group_id": group_id}):
        results.append({
            "user_id": row[0],
            "names": row[1],
            "alias_count": row[2],
        })
    
    return results


# ============================================================================
# SOCIAL NETWORK ANALYTICS
# ============================================================================

def get_mention_interaction_matrix(
    session: Session, group_id: str, limit: int = 20
) -> List[Dict[str, Any]]:
    """Get who mentions whom the most."""
    from ..db.models import Mention
    
    sql = text("""
    SELECT 
        m.user_id AS mentioner_id,
        m.name AS mentioner_name,
        mn.user_id AS mentioned_id,
        u.name AS mentioned_name,
        COUNT(*) AS mention_count
    FROM mentions mn
    JOIN messages m ON mn.message_id = m.id
    JOIN users u ON mn.user_id = u.id
    WHERE m.group_id = :group_id
    GROUP BY m.user_id, m.name, mn.user_id, u.name
    ORDER BY mention_count DESC
    LIMIT :limit;
    """)
    
    results = []
    for row in session.execute(sql, {"group_id": group_id, "limit": limit}):
        results.append({
            "mentioner_id": row[0],
            "mentioner_name": row[1] or "Unknown",
            "mentioned_id": row[2],
            "mentioned_name": row[3] or "Unknown",
            "mention_count": row[4],
        })
    
    return results


def get_reply_patterns(
    session: Session, group_id: str, time_window: int = 5, limit: int = 20
) -> List[Dict[str, Any]]:
    """Detect reply patterns (who responds to whom within time window in minutes)."""
    sql = text("""
    WITH message_pairs AS (
        SELECT 
            m1.user_id AS first_user_id,
            m1.name AS first_user_name,
            m2.user_id AS second_user_id,
            m2.name AS second_user_name,
            m1.created_at AS first_time,
            m2.created_at AS second_time,
            EXTRACT(EPOCH FROM (m2.created_at - m1.created_at)) / 60 AS gap_minutes
        FROM messages m1
        JOIN messages m2 ON m1.group_id = m2.group_id
        WHERE m1.group_id = :group_id
          AND m1.system = FALSE
          AND m2.system = FALSE
          AND m1.user_id != m2.user_id
          AND m2.created_at > m1.created_at
          AND EXTRACT(EPOCH FROM (m2.created_at - m1.created_at)) / 60 <= :time_window
    )
    SELECT 
        first_user_id,
        first_user_name,
        second_user_id,
        second_user_name,
        COUNT(*) AS reply_count,
        ROUND(AVG(gap_minutes)::numeric, 2) AS avg_response_time
    FROM message_pairs
    GROUP BY first_user_id, first_user_name, second_user_id, second_user_name
    ORDER BY reply_count DESC
    LIMIT :limit;
    """)
    
    results = []
    for row in session.execute(sql, {
        "group_id": group_id,
        "time_window": time_window,
        "limit": limit
    }):
        results.append({
            "first_user_id": row[0],
            "first_user_name": row[1] or "Unknown",
            "second_user_id": row[2],
            "second_user_name": row[3] or "Unknown",
            "reply_count": row[4],
            "avg_response_minutes": float(row[5]),
        })
    
    return results


# ============================================================================
# LEADERBOARDS
# ============================================================================

def get_night_owl_leaderboard(
    session: Session, group_id: str, limit: int = 10
) -> List[Dict[str, Any]]:
    """Users who post most between midnight and 5 AM."""
    sql = text("""
    SELECT 
        user_id,
        name,
        COUNT(*) AS night_messages,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS percentage
    FROM messages
    WHERE group_id = :group_id
      AND system = FALSE
      AND EXTRACT(HOUR FROM created_at) >= 0
      AND EXTRACT(HOUR FROM created_at) < 5
    GROUP BY user_id, name
    ORDER BY night_messages DESC
    LIMIT :limit;
    """)
    
    results = []
    for row in session.execute(sql, {"group_id": group_id, "limit": limit}):
        results.append({
            "user_id": row[0],
            "name": row[1] or "Unknown",
            "night_messages": row[2],
            "percentage": float(row[3]),
        })
    
    return results


def get_early_bird_leaderboard(
    session: Session, group_id: str, limit: int = 10
) -> List[Dict[str, Any]]:
    """Users who post most between 5 AM and 9 AM."""
    sql = text("""
    SELECT 
        user_id,
        name,
        COUNT(*) AS morning_messages,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS percentage
    FROM messages
    WHERE group_id = :group_id
      AND system = FALSE
      AND EXTRACT(HOUR FROM created_at) >= 5
      AND EXTRACT(HOUR FROM created_at) < 9
    GROUP BY user_id, name
    ORDER BY morning_messages DESC
    LIMIT :limit;
    """)
    
    results = []
    for row in session.execute(sql, {"group_id": group_id, "limit": limit}):
        results.append({
            "user_id": row[0],
            "name": row[1] or "Unknown",
            "morning_messages": row[2],
            "percentage": float(row[3]),
        })
    
    return results


def get_weekend_warrior_leaderboard(
    session: Session, group_id: str, limit: int = 10
) -> List[Dict[str, Any]]:
    """Users most active on weekends (Saturday/Sunday)."""
    sql = text("""
    WITH total_messages AS (
        SELECT user_id, name, COUNT(*) AS total
        FROM messages
        WHERE group_id = :group_id AND system = FALSE
        GROUP BY user_id, name
    ),
    weekend_messages AS (
        SELECT user_id, name, COUNT(*) AS weekend
        FROM messages
        WHERE group_id = :group_id 
          AND system = FALSE
          AND EXTRACT(DOW FROM created_at) IN (0, 6)
        GROUP BY user_id, name
    )
    SELECT 
        t.user_id,
        t.name,
        w.weekend AS weekend_messages,
        t.total AS total_messages,
        ROUND(100.0 * w.weekend / t.total, 1) AS weekend_percentage
    FROM total_messages t
    JOIN weekend_messages w ON t.user_id = w.user_id
    WHERE t.total >= 50  -- At least 50 messages
    ORDER BY weekend_percentage DESC
    LIMIT :limit;
    """)
    
    results = []
    for row in session.execute(sql, {"group_id": group_id, "limit": limit}):
        results.append({
            "user_id": row[0],
            "name": row[1] or "Unknown",
            "weekend_messages": row[2],
            "total_messages": row[3],
            "weekend_percentage": float(row[4]),
        })
    
    return results


def get_controversial_messages(
    session: Session, group_id: str, days: int = 30, limit: int = 10
) -> List[Dict[str, Any]]:
    """Messages with both high likes AND many replies (controversial)."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    
    sql = text("""
    WITH message_stats AS (
        SELECT 
            m.id,
            m.text,
            m.name,
            m.created_at,
            COUNT(DISTINCT mf.user_id) AS like_count,
            COUNT(DISTINCT m2.id) FILTER (
                WHERE m2.created_at > m.created_at 
                AND m2.created_at <= m.created_at + INTERVAL '10 minutes'
            ) AS reply_count
        FROM messages m
        LEFT JOIN message_favorites mf ON m.id = mf.message_id
        LEFT JOIN messages m2 ON m.group_id = m2.group_id
        WHERE m.group_id = :group_id
          AND m.system = FALSE
          AND m.created_at >= :cutoff
        GROUP BY m.id, m.text, m.name, m.created_at
    )
    SELECT 
        id,
        text,
        name,
        created_at,
        like_count,
        reply_count,
        like_count + reply_count AS controversy_score
    FROM message_stats
    WHERE like_count >= 3 AND reply_count >= 3
    ORDER BY controversy_score DESC
    LIMIT :limit;
    """)
    
    results = []
    for row in session.execute(sql, {"group_id": group_id, "cutoff": cutoff, "limit": limit}):
        results.append({
            "message_id": row[0],
            "text": row[1] or "(no text)",
            "name": row[2] or "Unknown",
            "created_at": row[3],
            "like_count": row[4],
            "reply_count": row[5],
            "controversy_score": row[6],
        })
    
    return results
