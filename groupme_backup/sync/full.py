"""Full backup logic for GroupMe messages.

Note: The IncrementalSyncEngine already handles full backups when
last_synced_message_id is None. This module provides a clearer
interface for explicitly requesting a full backup.
"""

import logging
from sqlalchemy.orm import Session

from ..api.client import GroupMeClient
from ..db.models import Group
from .incremental import IncrementalSyncEngine

logger = logging.getLogger(__name__)


class FullBackupEngine:
    """
    Engine for full message backup.

    This is a wrapper around IncrementalSyncEngine that ensures
    a complete backup is performed by resetting the sync state.
    """

    def __init__(self, api_client: GroupMeClient, db_session: Session):
        """
        Initialize the full backup engine.

        Args:
            api_client: GroupMe API client instance
            db_session: Database session
        """
        self.api = api_client
        self.db = db_session
        self.incremental_engine = IncrementalSyncEngine(api_client, db_session)

    def backup_group(self, group_id: str, force: bool = False) -> int:
        """
        Perform a full backup of a group.

        Args:
            group_id: The group ID to backup
            force: If True, reset sync state to force a complete re-backup

        Returns:
            Number of messages fetched
        """
        logger.info(f"Starting full backup for group {group_id}")

        # Get or create group
        group = self.db.query(Group).filter(Group.id == group_id).first()

        if force and group:
            logger.warning(
                f"Force flag set. Resetting sync state for group {group_id}"
            )
            group.last_synced_message_id = None
            group.last_synced_at = None
            self.db.commit()

        # Use incremental engine (it handles full backup when last_synced_message_id is None)
        return self.incremental_engine.sync_group(group_id)
