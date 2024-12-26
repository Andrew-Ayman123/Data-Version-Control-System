from database_connection import DatabaseConnection
from ingest_data import IngestData


def main():
    # Initialize database connection
    db = DatabaseConnection(
        {
            "type": "mssql",
            "server": r"(localdb)\test",
            "database": "cdc_management",
            "username": "andrew",
            "password": "123456789",
        }
    )

    # Create dataset instance
    ingest = IngestData()

    print("Starting data ingestion process...")

    # Step 1: Create initial dataset with Version 1
    print("\n=== Creating initial dataset with Version 1 ===")
    v1_id = db.insert_dataset_in_database(
        d_name="transactions",
        df=ingest.v1_data,
        description="Initial transaction dataset",
    )

    if v1_id is None:
        print("Failed to insert Version 1")
        return

    v1_loaded_data = db.get_version_data_by_columns("transactions", v1_id)
    if v1_loaded_data is None:
        print("Failed to retrieve Version 1 data")
        return

    # Step 2: Insert Version 2
    print("\n=== Inserting Version 2 ===")
    v2_id = db.insert_new_version(
        d_name="transactions",
        df=ingest.v2_data,
        description="Added region column and new transactions",
    )

    if v2_id:
        v2_loaded_data = db.get_version_data_by_columns("transactions", v2_id)
        if v2_loaded_data is None:
            print("Failed to retrieve Version 2 data")
            return
    else:
        print("Failed to insert Version 2")
        return

    # Step 3: Insert Version 3
    print("\n=== Inserting Version 3 ===")
    v3_id = db.insert_new_version(
        d_name="transactions",
        df=ingest.v3_data,
        description="Added sales_channel and price corrections",
    )

    if v3_id:
        v3_loaded_data = db.get_version_data_by_columns("transactions", v3_id)
        if v3_loaded_data is None:
            print("Failed to retrieve Version 3 data")
            return
    else:
        print("Failed to insert Version 3")
        return

    # Step 4: Insert Version 4 (duplicated of 3)
    print("\n=== Inserting Version 4 ===")
    v4_id = db.insert_new_version(
        d_name="transactions",
        df=ingest.v3_data,
        description="Added sales_channel and price corrections",
    )

    if v4_id:
        v4_loaded_data = db.get_version_data_by_columns("transactions", v4_id)
        if v4_loaded_data is None:
            print("Failed to retrieve Version 4 data")
            return
    else:
        print("Failed to insert Version 4")
        return

    print("\n=== Inserting Version 5 ===")
    v5_id = db.insert_new_version(
        d_name="transactions", df=ingest.v3_data, description="Duplicated Version 4"
    )

    if v5_id:
        v5_loaded_data = db.get_version_data_by_columns("transactions", v5_id)
        if v5_loaded_data is None:
            print("Failed to retrieve Version 5 data")
            return
    else:
        print("Failed to insert Version 5")
        return

    # Final verification
    print("\n=== Final Version Summary ===")
    versions_info = db.get_all_versions_info("transactions")
    for version in versions_info:
        print(f"\nVersion {version['version_id']}:")
        print(f"Created at: {version['created_at']}")
        print(f"Description: {version['description']}")
        print(f"Number of columns: {version['column_count']}")
        print(f"Columns: {', '.join(version['columns'])}")


def main2():
    # Initialize database connection
    db = DatabaseConnection()

    # Create dataset instance
    ingest = IngestData()

    # print("\n=== Inserting Version 4 ===")
    # v4_id = db.insert_new_version(
    #     d_name="transactions",
    #     df=ingest.v3_data,
    #     description="Added sales_channel and price corrections"
    # )

    # if v4_id:
    #     v4_loaded_data = db.get_version_data_by_columns("transactions", v4_id)
    #     if v4_loaded_data is None:
    #         print("Failed to retrieve Version 4 data")
    #         return
    # else:
    #     print("Failed to insert Version 4")
    #     return

    print(db.get_version_data_by_columns("transactions", 1))
    print(db.get_version_data_by_columns("transactions", 2))
    print(db.get_version_data_by_columns("transactions", 3))
    print(db.get_version_data_by_columns("transactions", 4))
    print(db.get_version_data_by_columns("transactions", 5))


if __name__ == "__main__":
    main()
