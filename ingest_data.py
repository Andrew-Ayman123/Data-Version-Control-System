import pandas as pd

class IngestData:
  # Version 1: Initial dataset (January 2024)
  v1_data = pd.DataFrame(
      {
          "customer_id": [1001, 1002, 1003, 1004, 1005],
          "transaction_id": [1, 2, 3, 4, 5],
          "transaction_date": [
              "2024-01-01",
              "2024-01-01",
              "2024-01-02",
              "2024-01-02",
              "2024-01-03",
          ],
          "product_id": [101, 102, 101, 103, 102],
          "quantity": [2, 1, 3, 2, 1],
          "price": [100.00, 200.00, 100.00, 150.00, 200.00],
      }
  )

  # Version 2: Added region column and new transactions
  v2_data = pd.DataFrame(
      {
          "customer_id": [1001, 1002, 1003, 1004, 1005, 1006, 1007],
          "transaction_id": [1, 2, 3, 4, 5, 6, 7],
          "transaction_date": [
              "2024-01-01",  # Existing records
              "2024-01-01",
              "2024-01-02",
              "2024-01-02",
              "2024-01-03",
              "2024-02-01",  # New records
              "2024-02-01",
          ],
          "product_id": [101, 102, 101, 103, 102, 101, 102],
          "quantity": [2, 1, 3, 2, 1, 2, 3],
          "price": [100.00, 200.00, 100.00, 150.00, 200.00, 100.00, 200.00],
          "region": ["North", "North", "South", "South", "East", "West", "West"],
      }
  )

  # Version 3: Added sales_channel and corrected some prices
  v3_data = pd.DataFrame(
      {
          "customer_id": [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009],
          "transaction_id": [1, 2, 3, 4, 5, 6, 7, 8, 9],
          "transaction_date": [
              "2024-01-01",  # Existing records with some corrections
              "2024-01-01",
              "2024-01-02",
              "2024-01-02",
              "2024-01-03",
              "2024-02-01",
              "2024-02-01",
              "2024-03-01",  # New records
              "2024-03-01",
          ],
          "product_id": [101, 102, 101, 103, 102, 101, 102, 101, 103],
          "quantity": [2, 1, 3, 2, 1, 2, 3, 2, 1],
          "price": [
              120.00,
              200.00,
              100.00,
              150.00,
              220.00,
              100.00,
              200.00,
              100.00,
              150.00,
          ],  # Note: price corrections for IDs 1 and 5
          "sales_channel": [
              "Online",
              "Store",
              "Online",
              "Store",
              "Online",
              "Store",
              "Online",
              "Store",
              "Online",
          ],
      }
  )
