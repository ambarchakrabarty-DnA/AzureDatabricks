**Implementing `metadata_config` as a Delta Table** in **Azure Databricks** allows for better scalability, schema enforcement, and ACID transactions. Here’s how you can set it up:

---

### **🔹 1️⃣ Create Metadata Delta Table in Azure Databricks**
Instead of an Azure SQL table, you can store metadata in **Delta Lake** for dynamic ETL execution.

```python
from pyspark.sql.types import StructType, StructField, StringType

# Define the schema for metadata
metadata_schema = StructType([
    StructField("TableName", StringType(), True),
    StructField("SourcePath", StringType(), True),
    StructField("TargetPath", StringType(), True),
    StructField("LoadFrequency", StringType(), True),
    StructField("TransformationLogic", StringType(), True)
])

# Create an empty Delta table
df_metadata = spark.createDataFrame([], metadata_schema)
df_metadata.write.format("delta").mode("overwrite").save("/mnt/bronze/metadata_config")
```

- This sets up an **empty Delta table** for **metadata-driven pipelines**.

---

### **🔸 2️⃣ Insert Metadata Records into Delta Table**
You can populate `metadata_config` dynamically from a **CSV, JSON, or manual inserts**.

#### **Example: Insert Metadata Records**
```python
metadata_records = [
    ("Customers", "abfss://bronze@datalake.dfs.core.windows.net/customers_raw/", 
     "/mnt/silver/customers", "Daily", "Remove duplicates & fix missing emails"),
    
    ("Products", "abfss://bronze@datalake.dfs.core.windows.net/products_raw/", 
     "/mnt/silver/products", "Weekly", "Standardize categories & prices")
]

df_insert = spark.createDataFrame(metadata_records, metadata_schema)
df_insert.write.format("delta").mode("append").save("/mnt/bronze/metadata_config")
```
- This adds **new metadata configurations** for **dynamic ingestion & transformation**.

---

### **🔹 3️⃣ Use Metadata in ETL Pipelines**
Instead of hardcoding file paths, **read from `metadata_config` Delta Table** dynamically.

```python
metadata_df = spark.read.format("delta").load("/mnt/bronze/metadata_config")

for row in metadata_df.collect():
    source_path = row["SourcePath"]
    target_path = row["TargetPath"]

    # Ingest data based on metadata config
    df = spark.read.format("csv").option("header", "true").load(source_path)
    df.write.format("delta").mode("overwrite").save(target_path)
```

- This **automates data ingestion & transformation** using **metadata stored in Delta Lake**.

---

### **🔶 4️⃣ Monitor Metadata Changes**
Since Delta Tables support **time travel**, you can track updates in metadata over time.

#### **Check Metadata History**
```python
df_history = spark.sql("DESCRIBE HISTORY delta.`/mnt/bronze/metadata_config`")
df_history.show(truncate=False)
```
- Allows **versioning** to track **changes in metadata**.

---


