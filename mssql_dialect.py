from sql_interface import SQLInterface

from typing import List, Optional, Dict


class MSSQLDialect(SQLInterface):
    """MSSQL implementation of SQL interface."""

    def create_database_if_not_exists(self, database_name: str) -> str:
        return f"""
            IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = '{database_name}')
            BEGIN
                CREATE DATABASE [{database_name}]
            END
        """

    def use_database(self, database_name: str) -> str:
        return f"USE [{database_name}]"

    def create_table_if_not_exists(
        self,
        table_name: str,
        columns: List[Dict[str, str]],
        foreign_keys: Optional[List[Dict[str, str]]] = None,
        primary_key: Optional[List[str]] = None,
    ) -> str:
        # Build column definitions
        column_defs = [f"[{col['name']}] {col['type']}" for col in columns]

        # Add primary key constraint if specified
        if primary_key:
            pk_columns = ", ".join(f"[{col}]" for col in primary_key)
            column_defs.append(f"PRIMARY KEY ({pk_columns})")

        # Add foreign key constraints if specified
        if foreign_keys:
            for fk in foreign_keys:
                constraint = (
                    f"CONSTRAINT [{fk['constraint_name']}] "
                    f"FOREIGN KEY ([{fk['column']}]) "
                    f"REFERENCES [{fk['reference_table']}]([{fk['reference_column']}])"
                )
                column_defs.append(constraint)

        columns_sql = ",\n                ".join(column_defs)

        return f"""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}')
            CREATE TABLE [{table_name}] (
                {columns_sql}
            )
        """

    def alter_table_add_columns(
        self, table_name: str, columns: List[Dict[str, str]]
    ) -> str:
        columns_sql = ", ".join([f"[{col['name']}] {col['type']}" for col in columns])
        return f"""
            ALTER TABLE [{table_name}]
            ADD {columns_sql}
        """

    def create_foreign_key_table(
        self, table_name: str, references: Dict[str, str]
    ) -> str:
        constraints = []
        columns = []
        for column, ref in references.items():
            table, ref_col = ref.split(".")
            columns.append(f"[{column}] int NOT NULL")
            constraints.append(
                f"FOREIGN KEY ([{column}]) REFERENCES [{table}]([{ref_col}])"
            )

        columns_sql = ", ".join(columns)
        constraints_sql = ", ".join(constraints)

        return f"""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}')
            CREATE TABLE [{table_name}] (
                {columns_sql},
                PRIMARY KEY ({', '.join(references.keys())}),
                {constraints_sql}
            )
        """

    def select_latest_version(self, table_name: str, order_by_column: str) -> str:
        return f"""
            SELECT TOP 1 *
            FROM [{table_name}]
            ORDER BY [{order_by_column}] DESC
        """

    def create_temporary_table(
        self, table_name: str, columns: List[Dict[str, str]]
    ) -> str:
        columns_sql = ", ".join([f"[{col['name']}] {col['type']}" for col in columns])
        return f"""
            CREATE TABLE #{table_name} (
                data_id int IDENTITY(1,1) PRIMARY KEY,
                {columns_sql}
            )
        """
