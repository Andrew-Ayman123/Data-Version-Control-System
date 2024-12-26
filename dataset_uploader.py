import pandas as pd
import streamlit as st
from typing import Optional
from database_connection import DatabaseConnection

class DatasetUploader:
    @staticmethod
    def upload_dataset(
        db_conn: DatabaseConnection, dataset_name: str, description: str, uploaded_file
    ) -> bool:
        try:
            df = pd.read_csv(uploaded_file)
            version_id = db_conn.insert_dataset_in_database(
                dataset_name, df, description
            )

            if version_id:
                st.success(
                    f"Dataset '{dataset_name}' created successfully with version {version_id}!"
                )
                return True
            st.error("Failed to create dataset.")
            return False
        except Exception as e:
            st.error(f"Error creating dataset: {str(e)}")
            return False

    @staticmethod
    def upload_new_version(
        db_conn: DatabaseConnection, dataset_name: str, description: str, uploaded_file
    ) -> bool:
        try:
            df = pd.read_csv(uploaded_file)
            version_id = db_conn.insert_new_version(dataset_name, df, description)

            if version_id:
                st.success(f"New version {version_id} created successfully!")
                return True
            st.error("Failed to create new version.")
            return False
        except Exception as e:
            st.error(f"Error creating new version: {str(e)}")
            return False
