import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import faker
from tqdm import tqdm

def generate_incremental_timestamps(start_date, end_date, interval_seconds=1):
    current_date = start_date
    timestamps = []

    while current_date <= end_date:
        timestamps.append(current_date.strftime("%Y-%m-%d %H:%M:%S"))
        current_date += timedelta(seconds=interval_seconds)

    return timestamps

# Seed for reproducability
random.seed(42) # Remove once validated 

# Create fake names
fake = faker.Faker()
unique_full_names = set()

while len(unique_full_names) < 200:
    full_name = fake.name()
    unique_full_names.add(full_name)

full_names = list(unique_full_names)

product_categories = [
    "Electronics","Clothing","Home and Furniture","Appliances","Toys and Games",
    "Beauty and Personal Care","Sports and Outdoors","Groceries","Health and Wellness","Automotive",
    "Office Supplies","Jewelry","Books and Magazines","Pet Supplies","Outdoor Living"
]

locations = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California",
    "Colorado", "Connecticut", "Delaware", "Florida", "Georgia",
    "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa",
    "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland",
    "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri",
    "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey",
    "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
    "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina",
    "South Dakota", "Tennessee", "Texas", "Utah", "Vermont",
    "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"
]

# Variables for the desired number of entries
start_timestamp = datetime(2023, 11, 15, 0, 0, 0)
end_timestamp = datetime(2023, 12, 1, 0, 0, 0)
timestamps = generate_incremental_timestamps(start_timestamp, end_timestamp)
num_products = 2000
num_stores = 250
num_fact_entries = len(timestamps)
initial_inventory_items = 200 # Number of items for every product


# Dimension Tables - Products
products_data = {
    "product_id": [f"P{i:03}" for i in range(1, num_products + 1)],
    "name": [f"Product{i}" for i in range(1, num_products + 1)],
    "category": [random.choice(product_categories) for i in range(1, num_products + 1)], #[f"Category{i % 5}" for i in range(1, num_products + 1)],
    "price": [round(random.uniform(20.0, 500.0), 2) for _ in range(num_products)],
    "supplier_id": [f"S{((i%3)):03}" for i in range(1, num_products + 1)]
}

# Dimension Tables - Stores
stores_data = {
    "store_id": [f"W{i:03}" for i in range(1, num_stores + 1)],
    "location": [random.choice(locations) for i in range(1, num_stores + 1)], #[f"Location{i}" for i in range(1, num_stores + 1)],
    "size": [random.randint(5000, 30000) for _ in range(num_stores)],
    "manager": [random.choice(full_names) for i in range(1, num_stores + 1)]
}

# Calculate product_id weights based on frequency in existing data
product_weights = [np.random.exponential(scale=1 / (products_data["product_id"].count(pid) + 1)) for pid in products_data["product_id"]]

# Calculate store_id weights based on frequency in existing data
store_weights = [np.random.exponential(scale=1 / (stores_data["store_id"].count(sid) + 1)) for sid in stores_data["store_id"]]

# Normalize weights to sum to 1
product_weights /= np.sum(product_weights)
store_weights /= np.sum(store_weights)

# Fact Tables - Sales Transactions Stream
sales_data = {
    "transaction_id": [f"T{i:04}" for i in range(1, num_fact_entries + 1)],
    "product_id": random.choices(products_data["product_id"], k=num_fact_entries, weights=product_weights),
    "timestamp": timestamps,
    "quantity": [random.randint(1, 5) for _ in range(num_fact_entries)],
    "unit_price": [],
    "store_id": random.choices(stores_data["store_id"], k=num_fact_entries, weights=store_weights)
}

# Assign matching unit prices from products_data
for product_id in sales_data["product_id"]:
    matching_price = products_data["price"][products_data["product_id"].index(product_id)]
    sales_data["unit_price"].append(round(matching_price, 2))

# Fact Tables - Inventory Updates Stream
# Inventory - Stock Initialization

inventory_data = []
can_restock = False
for product_id in products_data["product_id"]:
    for store_id in stores_data["store_id"]:
        timestamp = timestamps[0]
        quantity_change = initial_inventory_items

        inventory_data.append({
            "product_id": product_id,
            "timestamp": timestamp,
            "quantity_change": quantity_change,
            "store_id": store_id
        })

for i in tqdm(range(num_fact_entries), desc="Generating Inventory Updates"):#for i in range(num_inventory_entries):
    product_id = sales_data["product_id"][i]
    store_id = sales_data["store_id"][i]
    timestamp = sales_data["timestamp"][i]
    quantity_sold = sales_data["quantity"][i]
    quantity_change = -quantity_sold  # Deduct sold quantity from inventory

    inventory_data.append({
        "product_id": product_id,
        "timestamp": timestamp,
        "quantity_change": quantity_change,
        "store_id": store_id
    })

    tmp_ts = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    if tmp_ts.hour == 0 and tmp_ts.minute == 0 and tmp_ts.second == 0 and can_restock: # Restock at midnight
        for product_id in products_data["product_id"]:
            for store_id in stores_data["store_id"]:
                timestamp = sales_data["timestamp"][i]
                quantity_change = initial_inventory_items

                inventory_data.append({
                    "product_id": product_id,
                    "timestamp": timestamp,
                    "quantity_change": quantity_change,
                    "store_id": store_id
                })
    can_restock = True
# Create DataFrames
products_df = pd.DataFrame(products_data)
stores_df = pd.DataFrame(stores_data)
sales_df = pd.DataFrame(sales_data)
inventory_df = pd.DataFrame(inventory_data)

# Save DataFrames to CSV files
products_df.to_csv(f"products.csv", index=False)
stores_df.to_csv(f"stores.csv", index=False)
sales_df.to_csv(f"sales_transactions.csv", index=False)
inventory_df.to_csv(f"inventory_updates.csv", index=False)