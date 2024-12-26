import sqlalchemy
from sqlalchemy import text
import pandas as pd
import urllib
from typing import List, Optional, Dict
from sql_interface import SQLInterface
from mssql_dialect import MSSQLDialect


class DatabaseConnection:
    dtype_mapping = {
        "object": "varchar(255)",
        "int64": "bigint",
        "int32": "int",
        "float64": "float",
        "float32": "float",
        "datetime64[ns]": "datetime",
        "bool": "bit",
        "category": "varchar(255)",
        "string": "varchar(255)",
    }

    def __init__(self, conn_details: Dict):
        """Initialize database connection with connection details."""
        self.conn_details = conn_details
        self.sql = self._get_sql_dialect(conn_details["type"])
        self.engine = self._create_engine()
        self.connection = self.engine.connect()

        if conn_details.get("database") == "cdc_management":
            self.create_database()
        self.create_schema_tables()

    def _get_sql_dialect(self, db_type: str) -> SQLInterface:
        """Returns appropriate SQL dialect implementation."""
        if db_type == "mssql":
            return MSSQLDialect()
        # Add other dialect implementations as needed
        raise ValueError(f"Unsupported database type: {db_type}")

    def _create_engine(self):
        """Creates the appropriate database engine based on connection type."""
        db_type = self.conn_details["type"]

        if db_type == "mssql":
            return self._create_mssql_engine()
        elif db_type == "mysql":
            return self._create_mysql_engine()
        elif db_type == "postgres":
            return self._create_postgres_engine()
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

    def _create_mssql_engine(self):
        """Creates MS SQL Server engine."""
        connection_string = (
            f"Driver={{ODBC Driver 17 for SQL Server}};"
            f"Server={self.conn_details['server']};"
            f"UID={self.conn_details['username']};"
            f"PWD={self.conn_details['password']};"
        )

        if "database" in self.conn_details:
            connection_string += f"Database={self.conn_details['database']};"

        odbc_connect = urllib.parse.quote_plus(connection_string)
        return sqlalchemy.create_engine(
            f"mssql+pyodbc:///?odbc_connect={odbc_connect}",
            isolation_level="AUTOCOMMIT",
        )

    def _create_mysql_engine(self):
        """Creates MySQL engine."""
        return sqlalchemy.create_engine(
            f"mysql+pymysql://{self.conn_details['username']}:{self.conn_details['password']}"
            f"@{self.conn_details['server']}/{self.conn_details['database']}"
        )

    def _create_postgres_engine(self):
        """Creates PostgreSQL engine."""
        return sqlalchemy.create_engine(
            f"postgresql://{self.conn_details['username']}:{self.conn_details['password']}"
            f"@{self.conn_details['server']}/{self.conn_details['database']}"
        )

    def __del__(self):
        """Cleanup connection on object destruction."""
        if hasattr(self, "connection") and self.connection:
            self.connection.close()

    def execute(self, sql):
        return self.connection.execute(text(sql))

    def create_database(self):
        """Creates the CDC management database if it doesn't exist."""
        if self.conn_details["database"] == "cdc_management":
            self.execute(self.sql.create_database_if_not_exists("cdc_management"))
            self.execute(self.sql.use_database("cdc_management"))

    def create_schema_tables(self):
        """Creates all necessary tables in the database schema."""
        # Create Datasets table
        datasets_columns = [
            {"name": "d_name", "type": "varchar(20) NOT NULL PRIMARY KEY"},
            {"name": "d_description", "type": "varchar(50)"},
        ]
        self.execute(self.sql.create_table_if_not_exists("Datasets", datasets_columns))

        # Create Dataset Versions table
        versions_columns = [
            {"name": "dv_id", "type": "int IDENTITY(1,1)"},
            {"name": "dv_name", "type": "int NOT NULL"},
            {"name": "d_name", "type": "varchar(20) NOT NULL"},
            {
                "name": "dv_createdat",
                "type": "datetime NOT NULL DEFAULT CURRENT_TIMESTAMP",
            },
            {"name": "dv_description", "type": "varchar(50)"},
        ]
        versions_foreign_keys = [
            {
                "column": "d_name",
                "reference_table": "Datasets",
                "reference_column": "d_name",
                "constraint_name": "FK_DatasetVersions_Datasets",
            }
        ]
        self.execute(
            self.sql.create_table_if_not_exists(
                "Dataset_Versions",
                versions_columns,
                foreign_keys=versions_foreign_keys,
                primary_key=["dv_id"],
            )
        )

        # Create Column Definition table
        column_def_columns = [
            {"name": "cd_id", "type": "int IDENTITY(1,1)"},
            {"name": "dv_id", "type": "int NOT NULL"},
            {"name": "cd_column_name", "type": "varchar(20) NOT NULL"},
        ]
        column_def_foreign_keys = [
            {
                "column": "dv_id",
                "reference_table": "Dataset_Versions",
                "reference_column": "dv_id",
                "constraint_name": "FK_ColumnDefinition_DatasetVersions",
            }
        ]
        self.execute(
            self.sql.create_table_if_not_exists(
                "Column_Definition",
                column_def_columns,
                foreign_keys=column_def_foreign_keys,
                primary_key=["cd_id"],
            )
        )

    def create_dataset_table(self, d_name: str) -> bool:
        try:
            # Create main dataset table
            initial_columns = [{"name": "data_id", "type": "int IDENTITY(1,1)"}]
            self.execute(
                self.sql.create_table_if_not_exists(
                    d_name, initial_columns, primary_key=["data_id"]
                )
            )

            # Create connection table
            connection_columns = [
                {"name": "data_id", "type": "int NOT NULL"},
                {"name": "dv_id", "type": "int NOT NULL"},
            ]
            connection_foreign_keys = [
                {
                    "column": "data_id",
                    "reference_table": d_name,
                    "reference_column": "data_id",
                    "constraint_name": f"FK_{d_name}_connection_data",
                },
                {
                    "column": "dv_id",
                    "reference_table": "Dataset_Versions",
                    "reference_column": "dv_id",
                    "constraint_name": f"FK_{d_name}_connection_version",
                },
            ]
            self.execute(
                self.sql.create_table_if_not_exists(
                    f"{d_name}_connection",
                    connection_columns,
                    foreign_keys=connection_foreign_keys,
                    primary_key=["data_id", "dv_id"],
                )
            )

            return True
        except Exception as e:
            print(f"Error creating dataset tables: {str(e)}")
            return False

    def infer_sql_types(self, df: pd.DataFrame) -> List[Dict[str, str]]:
        """
        Infer SQL Server data types from pandas DataFrame columns.

        Args:
            df: Input pandas DataFrame

        Returns:
            List of dictionaries containing column names and their SQL Server types
        """
        columns = []
        for column_name, dtype in df.dtypes.items():
            # Get the string representation of the dtype
            dtype_str = str(dtype)

            # Handle special cases
            if dtype_str.startswith("datetime64"):
                sql_type = "datetime"
            elif (
                df[column_name].str.len().max() > 255
                if dtype_str == "object"
                else False
            ):
                sql_type = "varchar(MAX)"
            else:
                # Use the mapping dictionary with a default to varchar(255)
                sql_type = self.dtype_mapping.get(dtype_str, "varchar(255)")

            columns.append({"name": column_name, "type": sql_type})

        return columns

    def insert_dataset_in_database(
        self, d_name: str, df: pd.DataFrame, description: Optional[str] = None
    ) -> Optional[int]:
        """
        Inserts a new dataset into the Datasets table and creates a new version.

        Args:
            d_name: Name of the dataset
            description: Optional description of the dataset

        Returns:
            str: Dataset name if successful, None if failed
        """
        try:

            # Insert into Datasets table
            self.execute(
                f"""
                INSERT INTO Datasets (d_name, d_description)
                VALUES ('{d_name}', 'Initial dataset')
            """
            )

            # Create initial version
            self.execute(
                f"""
                INSERT INTO Dataset_Versions (d_name,dv_name, dv_description)
                VALUES ('{d_name}',1, 'Initial version')
            """
            )
            self.create_dataset_table(d_name)
            return self.insert_new_version(d_name, df, description)

        except Exception as e:
            print(f"Error inserting dataset: {str(e)}")
            return None

    def add_column_definitions(self, dv_id: int, column_names: List[str]) -> bool:
        """
        Adds column definitions for a dataset version.

        Args:
            dv_id: Dataset version ID
            column_names: List of column names to add

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            for column_name in column_names:
                self.execute(
                    f"""
                    INSERT INTO Column_Definition (dv_id, cd_column_name)
                    VALUES ({dv_id}, '{column_name}')
                """
                )
            return True
        except Exception as e:
            print(f"Error adding column definitions: {str(e)}")
            return False

    def get_latest_version_name(self, d_name: str) -> Optional[int]:
        """
        Gets the latest version details for a dataset.

        Args:
            d_name: Name of the dataset

        Returns:
            int: Latest version ID if exists, None otherwise
        """
        try:
            # select latest row version dv_name
            result = self.execute(
                f"""
                SELECT TOP 1 dv_name
                FROM Dataset_Versions
                WHERE d_name = '{d_name}'
                ORDER BY dv_createdat DESC
            """
            ).fetchone()

            return result[0] if result else None
        except Exception as e:
            print(f"Error getting latest version ID: {str(e)}")
            return None

    def get_existing_columns(self, d_name: str) -> List[str]:
        """
        Gets all existing columns for a dataset across all versions.

        Args:
            d_name: Name of the dataset

        Returns:
            List[str]: List of existing column names
        """
        try:
            result = self.execute(
                f"""
                SELECT DISTINCT cd_column_name
                FROM Column_Definition cd
                JOIN Dataset_Versions dv ON cd.dv_id = dv.dv_id
                WHERE dv.d_name = '{d_name}'
            """
            ).fetchall()
            return [row[0] for row in result]
        except Exception as e:
            print(f"Error getting existing columns: {str(e)}")
            return []

    def insert_new_version(
        self, d_name: str, df: pd.DataFrame, description: Optional[str] = None
    ) -> Optional[int]:
        """
        Inserts a new version of a dataset, handling data deduplication, relationships,
        and tracking column changes.

        Args:
            d_name: Name of the dataset
            df: DataFrame containing the new data
            description: Optional description of the new version

        Returns:
            int: New version ID if successful, None if failed
        """
        try:
            self.connection.commit()
            # Start transaction
            with self.connection.begin():

                # Create temporary staging table with same structure as main table
                staging_table = f"{d_name}_staging"
                columns = self.infer_sql_types(df)
                columns_sql = ", ".join(
                    [f"[{col['name']}] {col['type']}" for col in columns]
                )

                self.execute(
                    f"""
                    CREATE TABLE {staging_table} (
                        data_id int IDENTITY(1,1) PRIMARY KEY,
                        {columns_sql}
                    )
                """
                )

                # Upload data to staging table
                df.to_sql(
                    staging_table, self.connection, if_exists="append", index=False
                )

                latest_version_name = self.get_latest_version_name(d_name)
                # Create new version entry
                result = self.execute(
                    f"""
                    INSERT INTO Dataset_Versions (d_name,dv_name, dv_description)
                    OUTPUT INSERTED.dv_id
                    VALUES ('{d_name}',{latest_version_name+1}, '{description or "New version"}')
                """
                ).fetchone()
                new_version_id = result[0]

                # Get existing columns
                existing_columns = self.get_existing_columns(d_name)
                current_columns = df.columns.tolist()

                # Identify new columns
                new_columns = [
                    col for col in current_columns if col not in existing_columns
                ]

                # If there are new columns, alter the main table
                if new_columns:
                    new_columns_sql = []
                    for col in new_columns:
                        col_type = self.infer_sql_types(df[[col]])[0]["type"]
                        new_columns_sql.append(f"[{col}] {col_type}")

                    if new_columns_sql:
                        self.execute(
                            f"""
                            ALTER TABLE [{d_name}]
                            ADD {', '.join(new_columns_sql)}
                        """
                        )

                # Insert only non-duplicate records into main table
                insert_comparison_columns = [
                    col for col in current_columns if col in existing_columns
                ]
                if not new_columns and insert_comparison_columns:
                    comparison_clause = " AND ".join(
                        [
                            f"(m.[{col}] = s.[{col}] OR (m.[{col}] IS NULL AND s.[{col}] IS NULL))"
                            for col in insert_comparison_columns
                        ]
                    )
                else:
                    comparison_clause = (
                        "1=0"  # If no columns to compare, treat all records as new
                    )

                self.execute(
                    f"""
                    INSERT INTO [{d_name}] ({', '.join([f'[{col}]' for col in current_columns])})
                    SELECT {', '.join([f's.[{col}]' for col in current_columns])}
                    FROM {staging_table} s
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM [{d_name}] m
                        WHERE {comparison_clause}
                    )
                    """
                )

                insert_connection_comparison_clause = " AND ".join(
                    [
                        (
                            f"(m.[{col}] = s.[{col}] OR (m.[{col}] IS NULL AND s.[{col}] IS NULL))"
                            if col in current_columns
                            else f"m.[{col}] IS NULL"
                        )
                        for col in set(existing_columns + current_columns)
                    ]
                )

                # Insert connections for all relevant records
                self.execute(
                    f"""
                    INSERT INTO [{d_name}_connection] (data_id, dv_id)
                    SELECT m.data_id, {new_version_id}
                    FROM [{d_name}] m
                    JOIN {staging_table} s ON {insert_connection_comparison_clause}
                    """
                )

                # Clean up staging table
                self.execute(f"DROP TABLE {staging_table}")

                # Add column definitions for the new version
                self.add_column_definitions(new_version_id, current_columns)

                return new_version_id

        except Exception as e:
            print(f"Error inserting new version: {str(e)}")
            return None

    def get_version_columns(self, version_id: int) -> List[str]:
        """
        Gets the columns defined for a specific version.

        Args:
            version_id: Version ID to get columns for

        Returns:
            List[str]: List of column names defined for the version
        """
        try:
            result = self.execute(
                f"""
                SELECT cd_column_name
                FROM Column_Definition
                WHERE dv_id = {version_id}
                ORDER BY cd_id
            """
            ).fetchall()
            return [row[0] for row in result]
        except Exception as e:
            print(f"Error getting version columns: {str(e)}")
            return []

    def get_version_data_by_columns(
        self, d_name: str, version_id: int
    ) -> Optional[pd.DataFrame]:
        """
        Retrieves data for a specific version of a dataset using only the columns
        defined for that version.

        Args:
            d_name: Name of the dataset
            version_id: Version ID to retrieve

        Returns:
            Optional[pd.DataFrame]: DataFrame containing version data with appropriate columns,
                                or None if an error occurs
        """
        try:
            # Get columns defined for this version
            version_columns = self.get_version_columns(version_id)

            if not version_columns:
                print(f"No columns found for version {version_id}")
                return None

            # Build column selection string, including data_id
            columns_str = ", ".join([f"d.[{col}]" for col in version_columns])

            # Build and execute query
            query = f"""
                SELECT d.data_id, {columns_str}
                FROM [{d_name}] d
                JOIN [{d_name}_connection] c ON d.data_id = c.data_id
                WHERE c.dv_id = {version_id}
                ORDER BY d.data_id
            """

            # Use pandas to read the query result
            df = pd.read_sql(query, self.connection)

            # Get version metadata
            version_info = self.execute(
                f"""
                SELECT dv_createdat, dv_description
                FROM Dataset_Versions
                WHERE dv_id = {version_id}
            """
            ).fetchone()

            if version_info:
                print(f"\nVersion {version_id} Info:")
                print(f"Created at: {version_info[0]}")
                print(f"Description: {version_info[1]}")
                print(f"Number of columns: {len(version_columns)}")
                print(f"Columns: {', '.join(version_columns)}")
                print(f"Number of records: {len(df)}")

            return df

        except Exception as e:
            print(f"Error retrieving version data: {str(e)}")
            return None

    def get_all_versions_info(self, d_name: str) -> List[Dict]:
        """
        Gets information about all versions of a dataset.

        Args:
            d_name: Name of the dataset

        Returns:
            List[Dict]: List of dictionaries containing version information
        """
        try:
            versions = []
            result = self.execute(
                f"""
                SELECT dv_id,dv_name, dv_createdat, dv_description
                FROM Dataset_Versions
                WHERE d_name = '{d_name}'
                ORDER BY dv_createdat
            """
            ).fetchall()

            for row in result:
                version_id = row[0]
                columns = self.get_version_columns(version_id)

                versions.append(
                    {
                        "version_id": version_id,
                        "version_name": row[1],
                        "created_at": row[2],
                        "description": row[3],
                        "column_count": len(columns),
                    }
                )

            return versions

        except Exception as e:
            print(f"Error getting versions info: {str(e)}")
            return []
