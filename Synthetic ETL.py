# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window


# COMMAND ----------

tts = dbutils.widgets.get("tts")
print(tts)
output_path = f"/Volumes/workspace/default/sampledata/synthetic/processed/good_data_{tts}"
input_path = f"/Volumes/workspace/default/sampledata/synthetic/raw/retail_data_{tts}.csv"

# COMMAND ----------

# --------------------------------
# 1. READ DATA
# --------------------------------
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path)
)

# COMMAND ----------

# --------------------------------
# 2. BASIC CLEANING
# --------------------------------

# Standardize Fat Content
df_clean = df.withColumn(
    "Item_Fat_Content",
    when(lower(col("Item_Fat_Content")).isin("lf", "low fat"), "Low Fat")
    .when(lower(col("Item_Fat_Content")).isin("reg", "regular"), "Regular")
    .otherwise(col("Item_Fat_Content"))
)

# Fix Zero Visibility using avg per Item_Type
avg_vis = (
    df_clean.groupBy("Item_Type")
    .agg(avg("Item_Visibility").alias("avg_visibility"))
)

df_clean = (
    df_clean.join(broadcast(avg_vis), "Item_Type", "left")
    .withColumn(
        "Item_Visibility",
        when(col("Item_Visibility") == 0, col("avg_visibility"))
        .otherwise(col("Item_Visibility"))
    )
    .drop("avg_visibility")
)

# COMMAND ----------

# --------------------------------
# 3. FEATURE ENGINEERING
# --------------------------------

# Outlet Age
df_clean = df_clean.withColumn(
    "Outlet_Age",
    year(current_date()) - col("Outlet_Establishment_Year")
)

# Price Band
df_clean = df_clean.withColumn(
    "Price_Band",
    when(col("Item_MRP") < 50, "Low")
    .when(col("Item_MRP").between(50, 150), "Medium")
    .otherwise("High")
)

# COMMAND ----------

# --------------------------------
# 4. WINDOW FUNCTIONS
# --------------------------------

# Avg MRP per Item Type
w_item = Window.partitionBy("Item_Type")

df_clean = df_clean.withColumn(
    "Avg_MRP_Per_ItemType",
    avg("Item_MRP").over(w_item)
)

# Rank products by sales within each outlet
w_rank = Window.partitionBy("Outlet_Identifier").orderBy(desc("Item_Outlet_Sales"))

df_clean = df_clean.withColumn(
    "Sales_Rank_In_Outlet",
    rank().over(w_rank)
)


# COMMAND ----------

# --------------------------------
# 5. FILTERING (BUSINESS RULES)
# --------------------------------
df_filtered = df_clean.filter(
    (col("Item_Outlet_Sales") > 0) &
    (col("Item_MRP") > 0) &
    (col("Outlet_Age") >= 0)
)

# COMMAND ----------

# --------------------------------
# 6. COMPLEX AGGREGATION
# --------------------------------
df_agg = (
    df_filtered
    .groupBy(
        "Outlet_Identifier",
        "Outlet_Type",
        "Item_Type",
        "Price_Band"
    )
    .agg(
        sum("Item_Outlet_Sales").alias("Total_Sales"),
        avg("Item_Outlet_Sales").alias("Avg_Sales"),
        count("*").alias("Transaction_Count"),
        max("Item_MRP").alias("Max_MRP"),
        avg("Outlet_Age").alias("Avg_Outlet_Age")
    )
)


# COMMAND ----------

# --------------------------------
# 7. POST AGG FEATURES
# --------------------------------

# Revenue Contribution %
w_total = Window.partitionBy("Outlet_Identifier")

df_final = df_agg.withColumn(
    "Revenue_Contribution_Percent",
    round(
        (col("Total_Sales") / sum("Total_Sales").over(w_total)) * 100,
        2
    )
)

# COMMAND ----------

# --------------------------------
# 8. PERFORMANCE OPTIMIZATION
# --------------------------------

df_final = df_final.repartition("Outlet_Type")


# COMMAND ----------

# --------------------------------
# 9. SORTING & FINAL OUTPUT
# --------------------------------

df_final = df_final.orderBy(desc("Total_Sales"))

df_final.display()

# COMMAND ----------

# --------------------------------
# 10. WRITE OUTPUT
# --------------------------------

df_final.write.mode("overwrite").parquet(output_path)