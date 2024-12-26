<div align="center">

  <h1> Data Versioning Control System</h1>
  
  
  <img src="https://img.shields.io/badge/Python-14354C?style=for-the-badge&logo=python&logoColor=white" alt="Made with love in Egypt">
  <img src="https://img.shields.io/badge/streamlit-FF4B4B.svg?style=for-the-badge&logo=Streamlit&logoColor=white" alt="Made with love in Egypt">

  <img src="https://img.shields.io/badge/Made_With_Love-B32629?style=for-the-badge&logo=undertale&logoColor=white" alt="Made with love in Egypt">

  <img src="https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white" alt="Made with love in Egypt">

  <img src="https://img.shields.io/badge/SQLAlchemy-D71F00.svg?style=for-the-badge&logo=sqlalchemy&logoColor=white" alt="Made with love in Egypt">
  <h3>  Manage and Track Changes in Your Datasets with Ease!</h3>
  
  ```mermaid
  graph TB
    %% Main Components
    UI["🖥️ Streamlit UI"]
    DM["📊 Dataset Manager"]
    CM["🔌 Connection Manager"]
    DC["💾 Database Connection"]
    DU["⬆️ Dataset Uploader"]
    UIC["🎨 UI Components"]
    CF["⚙️ Config File"]
    DB[("🗄️ Database")]

    %% Relationships with labels
    UI -->|"User actions & navigation"| DM
    DM -->|"Get/Add connections"| CM
    DM -->|"Execute queries & manage data"| DC
    DM -->|"Handle dataset uploads"| DU
    DM -->|"Render forms & components"| UIC
    CM -->|"Read/Write connection details"| CF
    DC -->|"SQL queries & transactions"| DB
    DU -->|"Upload & version data"| DC

    %% Styling
    classDef component fill:#f9f9f9,stroke:#333,stroke-width:2px
    class UI,DM,CM,DC,DU,UIC,CF component

    %% Labels
    style DB fill:#f4f4f4,stroke:#666,stroke-width:2px

````

</div>

## :star2: About The Project

Here's a breakdown of the key components:

-   **Database Connections**: Manage connections to multiple databases including MS SQL Server, MySQL, and PostgreSQL. Easily add, remove, and view connection details.
-   **Dataset Management**: Upload new datasets and create new versions with detailed descriptions. Track changes across multiple versions and visualize schema and data differences.
-   **Version Control**: Maintain a history of dataset versions with metadata including creation date, description, and column definitions. Ensure data integrity and traceability.
-   **Schema Evolution**: Automatically detect and handle schema changes such as new columns. Ensure backward compatibility and seamless data integration.
-   **Data Visualization**: Preview dataset versions and visualize schema changes directly within the application. Utilize Streamlit's interactive components for a user-friendly experience.

## 📈 Diagrams

Sequence Diagram:

```mermaid
sequenceDiagram
  participant Client
  participant DatabaseConnection
  participant SQLDialect
  participant Engine
  participant Database

  Client->>DatabaseConnection: create(conn_details)
  activate DatabaseConnection
  DatabaseConnection->>SQLDialect: _get_sql_dialect(db_type)
  SQLDialect-->>DatabaseConnection: dialect instance

  DatabaseConnection->>DatabaseConnection: _create_engine()
  DatabaseConnection->>Engine: create_engine(connection_string)
  Engine-->>DatabaseConnection: engine instance

  DatabaseConnection->>Database: connect()
  Database-->>DatabaseConnection: connection


  DatabaseConnection->>Database: create_database_if_not_exist()
  DatabaseConnection->>Database: create_schema_tables_if_not_exist()


  deactivate DatabaseConnection

  Note over Client,Database: Dataset Operations

  Client->>DatabaseConnection: insert_dataset_in_database(name, df, desc)
  activate DatabaseConnection
  DatabaseConnection->>Database: Insert into Datasets
  DatabaseConnection->>Database: Create initial version
  DatabaseConnection->>Database: Create dataset table
  DatabaseConnection->>Database: Insert new version
  DatabaseConnection-->>Client: version_id
  deactivate DatabaseConnection

  Note over Client,Database: Version Operations

  Client->>DatabaseConnection: insert_new_version(name, df, desc)
  activate DatabaseConnection
  DatabaseConnection->>Database: Create staging table
  DatabaseConnection->>Database: Upload data to staging
  DatabaseConnection->>Database: Create version entry
  DatabaseConnection->>Database: Add column definitions
  DatabaseConnection->>Database: Insert non-duplicate records
  DatabaseConnection->>Database: Drop staging table
  DatabaseConnection-->>Client: new_version_id
  deactivate DatabaseConnection
````

Relational Schema:
<img src="./Assets/Data Schema.png" alt="0.Relational Schema.png">

## 🧰 Usage

-   Example Screenshots:
    <img src="./Assets/1.png" alt="First Image">
    <img src="./Assets/2.png" alt="First Image">
    <img src="./Assets/3.png" alt="First Image">
    <img src="./Assets/4.png" alt="First Image">
    <img src="./Assets/5.png" alt="First Image">
    <img src="./Assets/6.png" alt="First Image">

## 👩‍💻 TODO

-   Apply Tracking data type for column definition (add column in `column_definition` table and make the naming convention for the `d_name` table to be `columnName_type`)
-   Add Multiple connection types through createing more dialects and supported connections (MYSQL, Postgres)
-   Create Migration Script to migrate historical data between multiple databases/data warehouses
-   Create partitioning strategy to paralyze upload of the staging table and download of needed data.
-   Create a limit in the graphical view to limit the number of records that can show to the user (for performance)
