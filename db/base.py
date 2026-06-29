"""Compatibility shim for the old shared DB base import path."""

from db.persistence import PersistenceService

PostgresWriter = PersistenceService
