"""
Persistence Package

Handles storage of validation results.
"""

from .postgres_writer import PostgresValidationWriter, create_postgres_writer

__all__ = ["PostgresValidationWriter", "create_postgres_writer"]
