import streamlit as st
from typing import Optional
from connection_manager import ConnectionManager
from database_connection import DatabaseConnection
from dataset_uploader import DatasetUploader
from config import Config
from ui_components import UIComponents

class DatasetManager:
    def __init__(self, conn_manager: ConnectionManager):
        self.conn_manager = conn_manager

    def render_connections_page(self):
        st.header("Database Connections")
        self._display_existing_connections()
        self._handle_new_connection()

    def render_datasets_page(self):
        st.header("Datasets")
        selected_conn = self._select_connection()
        if not selected_conn:
            return

        try:
            db_conn = DatabaseConnection(self.conn_manager.connections[selected_conn])
            self._handle_datasets(db_conn)
        except Exception as e:
            st.error(f"Error connecting to database: {str(e)}")

    def _display_existing_connections(self):
        if self.conn_manager.connections:
            st.subheader("Existing Connections")
            for name, details in self.conn_manager.connections.items():
                col1, col2, col3 = st.columns([1, 3, 1])
                with col1:
                    st.write(Config.SUPPORTED_DB_TYPES[details["type"]])
                with col2:
                    st.write(f"**{name}**")
                    st.write(f"Server: {details['server']}")
                with col3:
                    if st.button("Remove", key=f"remove_{name}"):
                        self.conn_manager.remove_connection(name)
                        st.rerun()
                st.divider()

    def _handle_new_connection(self):
        new_conn = UIComponents.create_connection_form()
        if new_conn:
            self.conn_manager.add_connection(new_conn["name"], new_conn)
            st.success("Connection added successfully!")
            st.rerun()

    def _select_connection(self) -> Optional[str]:
        connection_names = list(self.conn_manager.connections.keys())
        if not connection_names:
            st.warning("Please add a database connection first.")
            return None
        return st.selectbox("Select Connection", connection_names)

    def _handle_datasets(self, db_conn: DatabaseConnection):
        self._handle_new_dataset(db_conn)
        self._display_existing_datasets(db_conn)

    def _handle_new_dataset(self, db_conn: DatabaseConnection):
        if st.button("➕ Add New Dataset"):
            st.session_state["adding_dataset"] = True

        if st.session_state.get("adding_dataset", False):
            with st.form("new_dataset_form"):
                dataset_name = st.text_input("Dataset Name")
                description = st.text_input("Description")
                uploaded_file = st.file_uploader("Choose CSV file", type="csv")
                submitted = st.form_submit_button("Create Dataset")

                if submitted and dataset_name and uploaded_file:
                    if DatasetUploader.upload_dataset(
                        db_conn, dataset_name, description, uploaded_file
                    ):
                        st.session_state["adding_dataset"] = False
                        st.rerun()

    def _display_existing_datasets(self, db_conn: DatabaseConnection):
        datasets = db_conn.execute(
            "SELECT d_name, d_description FROM Datasets"
        ).fetchall()

        if not datasets:
            st.info("No datasets found in this connection.")
            return

        for dataset in datasets:
            d_name, description = dataset
            self._render_dataset_section(db_conn, d_name, description)

    def _render_dataset_section(
        self, db_conn: DatabaseConnection, d_name: str, description: str
    ):
        with st.expander(f"📊 {d_name}"):
            st.write(f"Description: {description}")
            self._handle_new_version(db_conn, d_name)
            self._display_versions(db_conn, d_name)

    def _handle_new_version(self, db_conn: DatabaseConnection, d_name: str):
        if st.button("➕ Add New Version", key=f"new_version_{d_name}"):
            st.session_state[f"adding_version_{d_name}"] = True

        if st.session_state.get(f"adding_version_{d_name}", False):
            with st.form(f"new_version_{d_name}"):
                description = st.text_input("Version Description")
                uploaded_file = st.file_uploader("Choose CSV file", type="csv")
                submitted = st.form_submit_button("Create New Version")

                if submitted and uploaded_file:
                    if DatasetUploader.upload_new_version(
                        db_conn, d_name, description, uploaded_file
                    ):
                        st.session_state[f"adding_version_{d_name}"] = False
                        st.rerun()

    def _display_versions(self, db_conn: DatabaseConnection, d_name: str):
        versions = db_conn.get_all_versions_info(d_name)
        if not versions:
            return

        tabs = st.tabs([f"Version {v['version_name']}" for v in versions])
        for tab, version in zip(tabs, versions):
            with tab:
                st.write(f"Created: {version['created_at']}")
                st.write(f"Description: {version['description']}")
                st.write(f"Number of columns: {version['column_count']}")

                if st.button("Show Schema", key=f"{d_name}_v{version['version_name']}"):
                    UIComponents.display_version_schema(
                        db_conn, d_name, version["version_id"], version["version_name"]
                    )

                if st.button(
                    "Preview Data", key=f"{d_name}_v{version['version_name']}_preview"
                ):
                    df = db_conn.get_version_data_by_columns(
                        d_name, version["version_id"]
                    )
                    if df is not None:
                        st.dataframe(df.head())
