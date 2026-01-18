# Databricks notebook source
import random
import pandas as pd

# COMMAND ----------

tts = dbutils.widgets.get("tts")
print(tts)

# COMMAND ----------


# -----------------------------
# CONFIG
# -----------------------------
NUM_ROWS = 8000
OUTPUT_FILE = f"/Volumes/workspace/default/sampledata/synthetic/raw/retail_data_{tts}.csv"
print(OUTPUT_FILE)

ITEM_TYPES = [
    "Dairy", "Soft Drinks", "Meat", "Fruits and Vegetables", "Household",
    "Baking Goods", "Frozen Foods", "Snack Foods", "Health and Hygiene",
    "Hard Drinks", "Canned", "Breads", "Starchy Foods", "Breakfast", "Seafood"
]

OUTLET_TYPES = ["Supermarket Type1", "Supermarket Type2", "Supermarket Type3", "Grocery Store"]
OUTLET_SIZES = ["Small", "Medium", "High", None]
LOCATION_TYPES = ["Tier 1", "Tier 2", "Tier 3"]

FAT_CONTENT_VARIANTS = ["Low Fat", "low fat", "LF", "Regular", "reg"]


# COMMAND ----------

# -----------------------------
# HELPERS
# -----------------------------
def random_item_id():
    return f"ITEM{random.randint(1000, 9999)}"

def random_outlet_id():
    return f"OUT{random.randint(10, 99)}"


# COMMAND ----------

# -----------------------------
# DATA GENERATION
# -----------------------------
data = []

for _ in range(NUM_ROWS):
    item_type = random.choice(ITEM_TYPES)

    row = {
        "Item_Identifier": random_item_id(),
        "Item_Weight": round(random.uniform(4.0, 25.0), 2) if random.random() > 0.1 else None,  # 10% nulls
        "Item_Fat_Content": random.choice(FAT_CONTENT_VARIANTS),
        "Item_Visibility": round(random.uniform(0.0, 0.2), 4) if random.random() > 0.1 else 0.0,  # 10% zeros
        "Item_Type": item_type,
        "Item_MRP": round(random.uniform(30.0, 250.0), 2),
        "Outlet_Identifier": random_outlet_id(),
        "Outlet_Establishment_Year": random.randint(1985, 2020),
        "Outlet_Size": random.choice(OUTLET_SIZES),
        "Outlet_Location_Type": random.choice(LOCATION_TYPES),
        "Outlet_Type": random.choice(OUTLET_TYPES),
        "Item_Outlet_Sales": round(random.uniform(100.0, 5000.0), 2)
    }

    data.append(row)

# COMMAND ----------

# -----------------------------
# WRITE CSV
# -----------------------------
df = pd.DataFrame(data)
df.to_csv(OUTPUT_FILE, index=False)

print(f"✅ Generated {NUM_ROWS} rows into {OUTPUT_FILE}")