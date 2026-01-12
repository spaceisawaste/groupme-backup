"""GroupMe API client with rate limiting."""

import logging
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .exceptions import (
    AuthenticationError,
    GroupMeAPIError,
    NotFoundError,
    RateLimitError,
    ServerError,
)

logger = logging.getLogger(__name__)


class GroupMeClient:
    """
    GroupMe API client with built-in rate limiting.

    Implements a sliding window rate limiter to prevent exceeding API limits.
    """

    def __init__(
        self,
        access_token: str,
        base_url: str = "https://api.groupme.com/v3",
        rate_limit_calls: int = 100,
        rate_limit_period: int = 60,
    ):
        """
        Initialize the GroupMe API client.

        Args:
            access_token: GroupMe API access token
            base_url: Base URL for the GroupMe API
            rate_limit_calls: Maximum number of calls per rate limit period
            rate_limit_period: Rate limit period in seconds
        """
        self.access_token = access_token
        self.base_url = base_url.rstrip("/")
        self.rate_limit_calls = rate_limit_calls
        self.rate_limit_period = rate_limit_period

        # Sliding window for rate limiting
        self.request_times: deque = deque()

        # Set up session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _wait_for_rate_limit(self) -> None:
        """
        Implement sliding window rate limiting.

        Waits if necessary to stay within rate limits.
        """
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=self.rate_limit_period)

        # Remove old requests outside the window
        while self.request_times and self.request_times[0] < cutoff:
            self.request_times.popleft()

        # If at limit, wait until we can make another request
        if len(self.request_times) >= self.rate_limit_calls:
            sleep_time = (self.request_times[0] - cutoff).total_seconds()
            if sleep_time > 0:
                logger.warning(
                    f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds..."
                )
                time.sleep(sleep_time + 0.1)  # Small buffer
                # Recalculate after sleep
                now = datetime.now(timezone.utc)
                cutoff = now - timedelta(seconds=self.rate_limit_period)
                while self.request_times and self.request_times[0] < cutoff:
                    self.request_times.popleft()

        # Record this request
        self.request_times.append(now)

    def _make_request(
        self, method: str, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Make an API request with error handling.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (e.g., '/groups')
            params: Query parameters

        Returns:
            Parsed JSON response

        Raises:
            RateLimitError: If rate limit is exceeded
            AuthenticationError: If authentication fails
            NotFoundError: If resource is not found
            ServerError: If server error occurs
            GroupMeAPIError: For other API errors
        """
        self._wait_for_rate_limit()

        params = params or {}
        params["token"] = self.access_token

        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        logger.debug(f"Making {method} request to {url} with params: {params}")

        try:
            response = self.session.request(
                method, url, params=params, timeout=30
            )
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise GroupMeAPIError(f"Request failed: {e}")

        # Handle different status codes
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 304:
            # Not modified - no new data
            return {"response": {"messages": []}}
        elif response.status_code == 401:
            raise AuthenticationError("Invalid or expired access token")
        elif response.status_code == 404:
            raise NotFoundError(f"Resource not found: {endpoint}")
        elif response.status_code == 429:
            raise RateLimitError("Rate limit exceeded by API")
        elif response.status_code >= 500:
            raise ServerError(
                f"Server error: {response.status_code} - {response.text}"
            )
        else:
            raise GroupMeAPIError(
                f"API error: {response.status_code} - {response.text}",
                status_code=response.status_code,
            )

    def get_groups(self, page: int = 1, per_page: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch groups with pagination.

        Args:
            page: Page number (starts at 1)
            per_page: Number of groups per page (max 100)

        Returns:
            List of group dictionaries
        """
        params = {"page": page, "per_page": min(per_page, 100), "omit": "memberships"}
        data = self._make_request("GET", "/groups", params)
        return data.get("response", [])

    def get_all_groups(self) -> List[Dict[str, Any]]:
        """
        Fetch all groups across all pages.

        Returns:
            List of all group dictionaries
        """
        all_groups = []
        page = 1

        while True:
            groups = self.get_groups(page=page, per_page=100)
            if not groups:
                break
            all_groups.extend(groups)
            logger.info(f"Fetched {len(groups)} groups from page {page}")
            page += 1

            # GroupMe typically returns fewer than per_page when on last page
            if len(groups) < 100:
                break

        logger.info(f"Total groups fetched: {len(all_groups)}")
        return all_groups

    def get_group(self, group_id: str) -> Dict[str, Any]:
        """
        Fetch a single group by ID.

        Args:
            group_id: The group ID

        Returns:
            Group dictionary
        """
        data = self._make_request("GET", f"/groups/{group_id}")
        return data.get("response", {})

    def get_messages(
        self,
        group_id: str,
        before_id: Optional[str] = None,
        since_id: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Fetch messages from a group.

        Messages are returned in reverse chronological order (newest first).

        Args:
            group_id: The group ID
            before_id: Get messages before this message ID (for pagination)
            since_id: Get messages after this message ID (for incremental sync)
            limit: Maximum number of messages to fetch (max 100)

        Returns:
            List of message dictionaries
        """
        params: Dict[str, Any] = {"limit": min(limit, 100)}

        if before_id:
            params["before_id"] = before_id
        elif since_id:
            params["since_id"] = since_id

        data = self._make_request("GET", f"/groups/{group_id}/messages", params)
        messages = data.get("response", {}).get("messages", [])

        logger.debug(
            f"Fetched {len(messages)} messages from group {group_id} "
            f"(before_id={before_id}, since_id={since_id})"
        )

        return messages

    def get_user(self, user_id: str) -> Dict[str, Any]:
        """
        Fetch user information.

        Args:
            user_id: The user ID

        Returns:
            User dictionary
        """
        data = self._make_request("GET", f"/users/{user_id}")
        return data.get("response", {})
