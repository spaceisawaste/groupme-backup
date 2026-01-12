"""GroupMe API exceptions."""


class GroupMeAPIError(Exception):
    """Base exception for GroupMe API errors."""

    def __init__(self, message: str, status_code: int | None = None):
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)


class RateLimitError(GroupMeAPIError):
    """Exception raised when API rate limit is exceeded."""

    def __init__(self, message: str = "Rate limit exceeded"):
        super().__init__(message, status_code=429)


class AuthenticationError(GroupMeAPIError):
    """Exception raised for authentication failures."""

    def __init__(self, message: str = "Authentication failed"):
        super().__init__(message, status_code=401)


class NotFoundError(GroupMeAPIError):
    """Exception raised when resource is not found."""

    def __init__(self, message: str = "Resource not found"):
        super().__init__(message, status_code=404)


class ServerError(GroupMeAPIError):
    """Exception raised for server errors."""

    def __init__(self, message: str = "Server error occurred"):
        super().__init__(message, status_code=500)
