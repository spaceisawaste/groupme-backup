"""SQLAlchemy database models for GroupMe backup."""

from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all models."""

    pass


def utcnow() -> datetime:
    """Return current UTC timestamp."""
    return datetime.now(timezone.utc)


class Group(Base):
    """GroupMe group metadata."""

    __tablename__ = "groups"

    id: Mapped[str] = mapped_column(String(50), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    image_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    creator_user_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    type: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    share_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Incremental sync tracking
    last_synced_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_synced_message_id: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # Relationships
    messages: Mapped[List["Message"]] = relationship(
        "Message", back_populates="group", cascade="all, delete-orphan"
    )
    sync_logs: Mapped[List["SyncLog"]] = relationship(
        "SyncLog", back_populates="group", cascade="all, delete-orphan"
    )

    __table_args__ = (
        CheckConstraint("created_at IS NOT NULL", name="groups_created_at_check"),
        Index("idx_groups_last_synced", "last_synced_at"),
    )


class User(Base):
    """GroupMe user information."""

    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(50), primary_key=True)
    name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    avatar_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow, onupdate=utcnow
    )

    # Relationships
    messages: Mapped[List["Message"]] = relationship("Message", back_populates="user")
    favorites: Mapped[List["MessageFavorite"]] = relationship(
        "MessageFavorite", back_populates="user"
    )
    mentions: Mapped[List["Mention"]] = relationship("Mention", back_populates="user")


class Message(Base):
    """GroupMe message data."""

    __tablename__ = "messages"

    id: Mapped[str] = mapped_column(String(50), primary_key=True)
    group_id: Mapped[str] = mapped_column(
        String(50), ForeignKey("groups.id", ondelete="CASCADE"), nullable=False
    )
    user_id: Mapped[Optional[str]] = mapped_column(
        String(50), ForeignKey("users.id"), nullable=True
    )
    source_guid: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    system: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # Snapshot of sender info at message time
    name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    avatar_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    fetched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )

    # Relationships
    group: Mapped["Group"] = relationship("Group", back_populates="messages")
    user: Mapped[Optional["User"]] = relationship("User", back_populates="messages")
    favorites: Mapped[List["MessageFavorite"]] = relationship(
        "MessageFavorite", back_populates="message", cascade="all, delete-orphan"
    )
    attachments: Mapped[List["Attachment"]] = relationship(
        "Attachment", back_populates="message", cascade="all, delete-orphan"
    )
    mentions: Mapped[List["Mention"]] = relationship(
        "Mention", back_populates="message", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("idx_messages_group_id", "group_id"),
        Index("idx_messages_created_at", "created_at", postgresql_using="btree"),
        Index("idx_messages_user_id", "user_id"),
        Index("idx_messages_group_created", "group_id", "created_at"),
    )


class MessageFavorite(Base):
    """Message favorites/likes (many-to-many)."""

    __tablename__ = "message_favorites"

    message_id: Mapped[str] = mapped_column(
        String(50), ForeignKey("messages.id", ondelete="CASCADE"), primary_key=True
    )
    user_id: Mapped[str] = mapped_column(
        String(50), ForeignKey("users.id"), primary_key=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )

    # Relationships
    message: Mapped["Message"] = relationship("Message", back_populates="favorites")
    user: Mapped["User"] = relationship("User", back_populates="favorites")

    __table_args__ = (
        Index("idx_message_favorites_user_id", "user_id"),
        Index("idx_message_favorites_message", "message_id"),
    )


class Attachment(Base):
    """Message attachments (images, locations, emojis, etc.)."""

    __tablename__ = "attachments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    message_id: Mapped[str] = mapped_column(
        String(50), ForeignKey("messages.id", ondelete="CASCADE"), nullable=False
    )
    type: Mapped[str] = mapped_column(String(20), nullable=False)

    # Image fields
    url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Location fields
    location_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    latitude: Mapped[Optional[float]] = mapped_column(Numeric(10, 8), nullable=True)
    longitude: Mapped[Optional[float]] = mapped_column(Numeric(11, 8), nullable=True)

    # Split payment fields
    token: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Emoji fields
    placeholder: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    charmap: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    # Raw data for future-proofing
    raw_data: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )

    # Relationships
    message: Mapped["Message"] = relationship("Message", back_populates="attachments")

    __table_args__ = (
        Index("idx_attachments_message_id", "message_id"),
        Index("idx_attachments_type", "type"),
    )


class Mention(Base):
    """Message mentions (@mentions)."""

    __tablename__ = "mentions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    message_id: Mapped[str] = mapped_column(
        String(50), ForeignKey("messages.id", ondelete="CASCADE"), nullable=False
    )
    user_id: Mapped[str] = mapped_column(
        String(50), ForeignKey("users.id"), nullable=False
    )
    start_position: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    length: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )

    # Relationships
    message: Mapped["Message"] = relationship("Message", back_populates="mentions")
    user: Mapped["User"] = relationship("User", back_populates="mentions")

    __table_args__ = (
        Index("idx_mentions_message_id", "message_id"),
        Index("idx_mentions_user_id", "user_id"),
    )


class SyncLog(Base):
    """Sync operation log."""

    __tablename__ = "sync_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    group_id: Mapped[Optional[str]] = mapped_column(
        String(50), ForeignKey("groups.id"), nullable=True
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    messages_fetched: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="running")
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sync_type: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Relationships
    group: Mapped[Optional["Group"]] = relationship("Group", back_populates="sync_logs")
