from abc import ABC, abstractmethod
from typing import List, Optional, Dict

class SQLInterface(ABC):
    """Abstract interface for SQL operations across different database systems."""

    @abstractmethod
    def create_database_if_not_exists(self, database_name: str) -> str:
        """Returns SQL to create database if it doesn't exist."""
        pass

    @abstractmethod
    def use_database(self, database_name: str) -> str:
        """Returns SQL to switch to a specific database."""
        pass

    @abstractmethod
    def create_table_if_not_exists(
        self,
        table_name: str,
        columns: List[Dict[str, str]],
        foreign_keys: Optional[List[Dict[str, str]]] = None,
        primary_key: Optional[List[str]] = None,
    ) -> str:
        """
        Returns SQL to create table with specified columns and constraints if it doesn't exist.

        Args:
            table_name: Name of the table to create
            columns: List of dictionaries with 'name' and 'type' keys
            foreign_keys: Optional list of foreign key constraints, each with:
                - column: The column in this table
                - reference_table: The referenced table
                - reference_column: The referenced column
                - constraint_name: Name for the foreign key constraint
            primary_key: Optional list of column names that form the primary key
        """
        pass

    @abstractmethod
    def alter_table_add_columns(
        self, table_name: str, columns: List[Dict[str, str]]
    ) -> str:
        """Returns SQL to add new columns to existing table."""
        pass

    @abstractmethod
    def create_foreign_key_table(
        self, table_name: str, references: Dict[str, str]
    ) -> str:
        """Returns SQL to create a table with foreign key constraints."""
        pass

    @abstractmethod
    def select_latest_version(self, table_name: str, order_by_column: str) -> str:
        """Returns SQL to select the latest version from a table."""
        pass

    @abstractmethod
    def create_temporary_table(
        self, table_name: str, columns: List[Dict[str, str]]
    ) -> str:
        """Returns SQL to create a temporary table."""
        pass
