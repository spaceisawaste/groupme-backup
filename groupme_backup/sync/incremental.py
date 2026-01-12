"""Incremental sync engine for GroupMe messages."""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from ..api.client import GroupMeClient
from ..db.models import Attachment, Group, Message, MessageFavorite, Mention, User

logger = logging.getLogger(__name__)


class IncrementalSyncEngine:
    """
    Engine for incremental message synchronization.

    Tracks the last synced message ID and only fetches new messages.
    """

    def __init__(self, api_client: GroupMeClient, db_session: Session, fast_mode: bool = False):
        """
        Initialize the incremental sync engine.

        Args:
            api_client: GroupMe API client instance
            db_session: Database session
            fast_mode: Enable fast mode (removes safety checks, larger batches, bulk inserts)
        """
        self.api = api_client
        self.db = db_session
        self.fast_mode = fast_mode

    def sync_group(self, group_id: str) -> int:
        """
        Perform incremental sync for a group.

        Args:
            group_id: The group ID to sync

        Returns:
            Number of new messages fetched

        Raises:
            ValueError: If group not found in database
        """
        # Get group from database
        group = self.db.query(Group).filter(Group.id == group_id).first()
        if not group:
            # Group doesn't exist, fetch from API and create
            logger.info(f"Group {group_id} not in database, fetching metadata...")
            group_data = self.api.get_group(group_id)
            group = self._create_group(group_data)
            self.db.add(group)
            self.db.commit()

        last_message_id = group.last_synced_message_id
        new_messages_count = 0

        logger.info(
            f"Starting incremental sync for group {group_id} "
            f"(last_synced_message_id={last_message_id})"
        )

        # Fetch new messages
        all_new_messages = []

        if last_message_id:
            # Incremental: fetch messages after last synced
            logger.info(f"Fetching messages since {last_message_id}")
            messages = self.api.get_messages(group_id, since_id=last_message_id)
            all_new_messages = messages
        else:
            # First sync: fetch all messages with pagination
            logger.info(f"First sync for group {group_id}, fetching all messages")
            before_id = None

            while True:
                messages = self.api.get_messages(
                    group_id, before_id=before_id, limit=100
                )
                if not messages:
                    break

                all_new_messages.extend(messages)
                before_id = messages[-1]["id"]  # Oldest message in batch

                logger.info(
                    f"Fetched batch of {len(messages)} messages "
                    f"(total so far: {len(all_new_messages)})"
                )

                # Check if we got fewer than requested (indicates last page)
                if len(messages) < 100:
                    break

        if not all_new_messages:
            logger.info(f"No new messages for group {group_id}")
            return 0

        # Messages come newest-first, reverse to process oldest-first
        all_new_messages.reverse()
        logger.info(
            f"Processing {len(all_new_messages)} messages "
            f"in chronological order (oldest to newest)"
        )

        # Process and store messages in batches
        # Fast mode uses larger batches for better performance
        batch_size = 5000 if self.fast_mode else 1000

        if self.fast_mode:
            logger.info(f"Fast mode enabled: batch_size={batch_size}, flush disabled")

        for i, msg_data in enumerate(all_new_messages, 1):
            if i % 100 == 0:
                logger.info(f"Processed {i}/{len(all_new_messages)} messages")

            self._store_message(msg_data, group_id)
            new_messages_count += 1

            # Commit in batches to handle interruptions gracefully
            if i % batch_size == 0 or i == len(all_new_messages):
                # Update last synced message ID to this batch
                current_message_id = msg_data["id"]
                group.last_synced_message_id = current_message_id
                group.last_synced_at = datetime.now(timezone.utc)

                self.db.commit()
                logger.info(
                    f"Committed batch: {i}/{len(all_new_messages)} messages "
                    f"(last_synced_message_id={current_message_id})"
                )

        logger.info(
            f"Completed sync for group {group_id}. "
            f"Fetched {new_messages_count} new messages. "
            f"Last message ID: {group.last_synced_message_id}"
        )

        return new_messages_count

    def _create_group(self, group_data: Dict[str, Any]) -> Group:
        """
        Create a Group model from API data.

        Args:
            group_data: Group data from GroupMe API

        Returns:
            Group model instance
        """
        created_timestamp = group_data.get("created_at")
        created_at = None
        if created_timestamp:
            created_at = datetime.fromtimestamp(created_timestamp, tz=timezone.utc)

        updated_timestamp = group_data.get("updated_at")
        updated_at = None
        if updated_timestamp:
            updated_at = datetime.fromtimestamp(updated_timestamp, tz=timezone.utc)

        return Group(
            id=group_data["id"],
            name=group_data.get("name", "Unknown"),
            description=group_data.get("description"),
            image_url=group_data.get("image_url"),
            creator_user_id=group_data.get("creator_user_id"),
            created_at=created_at or datetime.now(timezone.utc),
            updated_at=updated_at,
            type=group_data.get("type"),
            share_url=group_data.get("share_url"),
        )

    def _store_message(self, msg_data: Dict[str, Any], group_id: str) -> None:
        """
        Store a single message with all metadata.

        Args:
            msg_data: Message data from GroupMe API
            group_id: The group ID
        """
        # Create or update user
        if msg_data.get("user_id"):
            user = self.db.query(User).filter(User.id == msg_data["user_id"]).first()

            if not user:
                user = User(
                    id=msg_data["user_id"],
                    name=msg_data.get("name"),
                    avatar_url=msg_data.get("avatar_url"),
                )
                self.db.add(user)
            else:
                # Update user info
                user.last_seen_at = datetime.now(timezone.utc)
                if msg_data.get("name"):
                    user.name = msg_data["name"]
                if msg_data.get("avatar_url"):
                    user.avatar_url = msg_data["avatar_url"]

        # Check if message already exists (avoid duplicates)
        existing = self.db.query(Message).filter(Message.id == msg_data["id"]).first()
        if existing:
            logger.debug(f"Message {msg_data['id']} already exists, skipping")
            return

        # Create message
        created_timestamp = msg_data.get("created_at")
        created_at = datetime.fromtimestamp(created_timestamp, tz=timezone.utc)

        message = Message(
            id=msg_data["id"],
            group_id=group_id,
            user_id=msg_data.get("user_id"),
            source_guid=msg_data.get("source_guid"),
            created_at=created_at,
            text=msg_data.get("text"),
            system=msg_data.get("system", False),
            name=msg_data.get("name"),
            avatar_url=msg_data.get("avatar_url"),
        )
        self.db.add(message)

        # Store favorites/likes
        for user_id in msg_data.get("favorited_by", []):
            # Ensure user exists
            fav_user = self.db.query(User).filter(User.id == user_id).first()
            if not fav_user:
                fav_user = User(id=user_id)
                self.db.add(fav_user)

            favorite = MessageFavorite(message_id=msg_data["id"], user_id=user_id)
            self.db.add(favorite)

        # Store attachments
        for attachment_data in msg_data.get("attachments", []):
            self._store_attachment(msg_data["id"], attachment_data)

        # Flush to catch any errors before committing the batch (disabled in fast mode)
        if not self.fast_mode:
            self.db.flush()

    def _store_attachment(
        self, message_id: str, attachment_data: Dict[str, Any]
    ) -> None:
        """
        Store an attachment with all metadata.

        Args:
            message_id: The message ID
            attachment_data: Attachment data from GroupMe API
        """
        attachment_type = attachment_data.get("type")

        attachment = Attachment(
            message_id=message_id,
            type=attachment_type,
            raw_data=attachment_data,  # Store complete JSON for future-proofing
        )

        # Handle type-specific fields
        if attachment_type == "image":
            attachment.url = attachment_data.get("url")
        elif attachment_type == "location":
            attachment.location_name = attachment_data.get("name")
            attachment.latitude = attachment_data.get("lat")
            attachment.longitude = attachment_data.get("lng")
        elif attachment_type == "split":
            attachment.token = attachment_data.get("token")
        elif attachment_type == "emoji":
            attachment.placeholder = attachment_data.get("placeholder")
            attachment.charmap = attachment_data.get("charmap")
        elif attachment_type == "mentions":
            # Mentions are stored separately in mentions table
            self._store_mentions(message_id, attachment_data)
            return  # Don't store mentions as attachment

        self.db.add(attachment)

    def _store_mentions(
        self, message_id: str, mentions_data: Dict[str, Any]
    ) -> None:
        """
        Store mentions from a message.

        Args:
            message_id: The message ID
            mentions_data: Mentions attachment data from GroupMe API
        """
        user_ids = mentions_data.get("user_ids", [])
        loci = mentions_data.get("loci", [])

        for i, user_id in enumerate(user_ids):
            # Ensure user exists
            mentioned_user = self.db.query(User).filter(User.id == user_id).first()
            if not mentioned_user:
                mentioned_user = User(id=user_id)
                self.db.add(mentioned_user)

            # Get position info if available
            start_position = None
            length = None
            if i < len(loci):
                loci_item = loci[i]
                if isinstance(loci_item, list) and len(loci_item) == 2:
                    start_position, length = loci_item

            mention = Mention(
                message_id=message_id,
                user_id=user_id,
                start_position=start_position,
                length=length,
            )
            self.db.add(mention)
