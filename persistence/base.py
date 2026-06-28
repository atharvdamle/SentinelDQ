"""Compatibility shim for the old shared DB base import path."""

from persistence.persistence import PersistenceService

PostgresWriter = PersistenceService
