import streamlit as st
import pandas as pd
from typing import Dict, Optional
from config import Config
from database_connection import DatabaseConnection

class UIComponents:
    @staticmethod
    def create_connection_form() -> Optional[Dict]:
        st.subheader("Add New Connection")
        with st.form("connection_form"):
            name = st.text_input("Connection Name")
            db_type = st.selectbox(
                "Database Type", list(Config.SUPPORTED_DB_TYPES.keys())
            )
            server = st.text_input("Server")
            database = st.text_input("Database")
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")

            submitted = st.form_submit_button("Save Connection")
            if submitted and all([name, server, database, username, password]):
                return {
                    "name": name,
                    "type": db_type,
                    "server": server,
                    "database": database,
                    "username": username,
                    "password": password,
                }
        return None

    @staticmethod
    def display_version_schema(
        db_conn: DatabaseConnection,
        dataset_name: str,
        version_id: int,
        version_name: int,
    ):
        st.subheader(f"Schema for Version {version_name}")
        columns = db_conn.get_version_columns(version_id)

        if columns:
            df = pd.DataFrame(columns, columns=["Column Name"])
            st.dataframe(
                df.style.set_properties(**{"text-align": "left"}).set_table_styles(
                    [{"selector": "th", "props": [("text-align", "left")]}]
                )
            )
        else:
            st.warning("No schema information available for this version.")
