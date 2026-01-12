"""Groups cache for numeric indexing."""

import json
from pathlib import Path
from typing import Dict, List, Optional

CACHE_FILE = Path.home() / ".groupme_backup_groups.json"


def save_groups_cache(groups: List[Dict]) -> None:
    """Save groups to cache file."""
    CACHE_FILE.write_text(json.dumps(groups, indent=2))


def load_groups_cache() -> List[Dict]:
    """Load groups from cache file."""
    if not CACHE_FILE.exists():
        return []
    try:
        return json.loads(CACHE_FILE.read_text())
    except Exception:
        return []


def get_group_by_index(index: int) -> Optional[Dict]:
    """Get group by numeric index (1-based)."""
    groups = load_groups_cache()
    if 0 < index <= len(groups):
        return groups[index - 1]
    return None


def get_group_id_by_index(index: int) -> Optional[str]:
    """Get group ID by numeric index (1-based)."""
    group = get_group_by_index(index)
    return group["id"] if group else None
