import streamlit as st
from connection_manager import ConnectionManager
from dataset_manager import DatasetManager

def main():
    st.title("Dataset Manager")
    st.sidebar.title("Navigation")

    conn_manager = ConnectionManager()
    dataset_manager = DatasetManager(conn_manager)

    page = st.sidebar.radio("Go to", ["Connections", "Datasets"])

    if page == "Connections":
        dataset_manager.render_connections_page()
    else:
        dataset_manager.render_datasets_page()


if __name__ == "__main__":
    main()
