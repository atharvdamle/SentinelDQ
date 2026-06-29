"""Compatibility export for the shared persistence service."""

from __future__ import annotations

from db.persistence import PersistenceService

DatabaseService = PersistenceService

__all__ = ["PersistenceService", "DatabaseService"]
