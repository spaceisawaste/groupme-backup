"""Main sync orchestration engine."""

import logging
from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy.orm import Session
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ..api.client import GroupMeClient
from ..api.exceptions import GroupMeAPIError, RateLimitError
from ..db.models import SyncLog
from .incremental import IncrementalSyncEngine

logger = logging.getLogger(__name__)


class SyncEngine:
    """
    Main sync orchestration engine with error handling and logging.
    """

    def __init__(self, api_client: GroupMeClient, db_session: Session, fast_mode: bool = False):
        """
        Initialize the sync engine.

        Args:
            api_client: GroupMe API client instance
            db_session: Database session
            fast_mode: Enable fast mode for large backups (3-5x faster, less safe)
        """
        self.api = api_client
        self.db = db_session
        self.fast_mode = fast_mode
        self.incremental_engine = IncrementalSyncEngine(api_client, db_session, fast_mode)

    def sync_group_with_retry(
        self, group_id: str, max_retries: int = 3
    ) -> tuple[int, Optional[str]]:
        """
        Sync a group with retry logic.

        Args:
            group_id: The group ID to sync
            max_retries: Maximum number of retry attempts

        Returns:
            Tuple of (messages_fetched, error_message)
        """

        @retry(
            stop=stop_after_attempt(max_retries),
            wait=wait_exponential(multiplier=1, min=4, max=60),
            retry=retry_if_exception_type((GroupMeAPIError, RateLimitError)),
        )
        def _sync_with_retry():
            return self.incremental_engine.sync_group(group_id)

        started_at = datetime.now(timezone.utc)

        try:
            messages_fetched = _sync_with_retry()

            # Create sync log after successful sync (group now exists in DB)
            sync_log = SyncLog(
                group_id=group_id,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                messages_fetched=messages_fetched,
                status="completed",
                sync_type="incremental",
            )
            self.db.add(sync_log)
            self.db.commit()

            logger.info(
                f"Successfully synced group {group_id}. "
                f"Fetched {messages_fetched} messages."
            )
            return messages_fetched, None

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Failed to sync group {group_id}: {error_msg}")

            # Try to create a failed sync log (group may or may not exist)
            try:
                sync_log = SyncLog(
                    group_id=group_id,
                    started_at=started_at,
                    completed_at=datetime.now(timezone.utc),
                    messages_fetched=0,
                    status="failed",
                    error_message=error_msg,
                    sync_type="incremental",
                )
                self.db.add(sync_log)
                self.db.commit()
            except Exception:
                # If we can't create the log (group doesn't exist), that's okay
                logger.debug("Could not create sync log (group may not exist in DB)")

            return 0, error_msg

    def sync_multiple_groups(
        self, group_ids: List[str]
    ) -> dict[str, tuple[int, Optional[str]]]:
        """
        Sync multiple groups.

        Args:
            group_ids: List of group IDs to sync

        Returns:
            Dictionary mapping group_id to (messages_fetched, error_message)
        """
        results = {}

        for group_id in group_ids:
            logger.info(f"Syncing group {group_id}...")
            messages_fetched, error = self.sync_group_with_retry(group_id)
            results[group_id] = (messages_fetched, error)

        return results

    def sync_all_groups(self) -> dict[str, tuple[int, Optional[str]]]:
        """
        Sync all groups accessible to the API token.

        Returns:
            Dictionary mapping group_id to (messages_fetched, error_message)
        """
        logger.info("Fetching all groups...")
        groups = self.api.get_all_groups()
        group_ids = [g["id"] for g in groups]

        logger.info(f"Found {len(group_ids)} groups. Starting sync...")
        return self.sync_multiple_groups(group_ids)
