"""Database session management."""

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from ..config.settings import get_settings

# Global engine and session factory
_engine = None
_SessionLocal = None


def get_engine():
    """Get or create the global database engine."""
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_engine(
            settings.database_url,
            echo=False,  # Set to True for SQL query logging during development
            pool_pre_ping=True,  # Verify connections before using them
            pool_size=5,
            max_overflow=10,
        )
    return _engine


def get_session_factory():
    """Get or create the global session factory."""
    global _SessionLocal
    if _SessionLocal is None:
        engine = get_engine()
        _SessionLocal = sessionmaker(
            bind=engine,
            autocommit=False,
            autoflush=False,
            expire_on_commit=False,
        )
    return _SessionLocal


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """
    Context manager for database sessions.

    Usage:
        with get_session() as session:
            user = session.query(User).first()
    """
    SessionLocal = get_session_factory()
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def create_session() -> Session:
    """
    Create a new database session.

    Note: Caller is responsible for closing the session.
    Prefer using get_session() context manager when possible.
    """
    SessionLocal = get_session_factory()
    return SessionLocal()
