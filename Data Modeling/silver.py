# Databricks notebook source
# MAGIC %md
# MAGIC catalog and scehma names

# COMMAND ----------

catalog='datamodeling'
schema='data_modeling'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Users Table

# COMMAND ----------

bronze_users=f"{catalog}.{schema}.users"
df_users=spark.table(bronze_users)

# COMMAND ----------

df_users.display()

# COMMAND ----------

df_users.printSchema()

# COMMAND ----------

df_users=df_users.withColumn("user_id",df_users["user_id"].cast("long"))

# COMMAND ----------

df_users.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC check for null values count if drop it in user_id

# COMMAND ----------

from pyspark.sql.functions import col, sum,trim

# COMMAND ----------

# Count nulls per column
df_users.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_users.columns]).show()


# COMMAND ----------

df_users.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC trim leading whitesapces in name col

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

df_users = df_users.withColumn("email",regexp_replace(col("email"), "_",""))

# COMMAND ----------

df_users.display()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace,concat,expr,lpad

df_users = df_users.withColumn("phone",regexp_replace(col("phone"), "[^0-9]",""))


# COMMAND ----------

display(df_users)

# COMMAND ----------

df_users=df_users.withColumn("phone",lpad(col("phone"),10,"0"))

# COMMAND ----------

df_users = df_users.withColumn(
    "phone",
    expr("concat('+1-', substr(phone,1,3), '-', substr(phone,4,3), '-', substr(phone,7,4))")
)

# COMMAND ----------

display(df_users)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Address Table

# COMMAND ----------

bronze_address=f"{catalog}.{schema}.address"
df_address = spark.read.table(bronze_address)

# COMMAND ----------

display(df_address)

# COMMAND ----------

# MAGIC %md
# MAGIC ## check the count of null

# COMMAND ----------

from pyspark.sql.functions import col, sum, when

display(
  df_address.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_address.columns
  ])
)

# COMMAND ----------

# MAGIC %md
# MAGIC drop null value in address table

# COMMAND ----------

df_address=df_address.dropna()



# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove the new line(dropdown)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, trim, col

df_address = df_address.withColumn(
    "address",
    trim(regexp_replace(col("address"), "\n", " "))
)


# COMMAND ----------

df_address.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orders table

# COMMAND ----------

bronze_orders=f"{catalog}.{schema}.orders"
df_orders=spark.table(bronze_orders)

# COMMAND ----------

display(df_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ### the col order_placed_date change the unix time stamp to readable dates isung unixtime

# COMMAND ----------

from pyspark.sql.functions import col, from_unixtime

df_orders = df_orders.withColumn(
    "order_placed_date",
    from_unixtime(col("order_placed_date").cast("double")).cast("timestamp")
)

display(df_orders)


# COMMAND ----------

# MAGIC %md
# MAGIC ### driver table

# COMMAND ----------

bronze_drivers=f"{catalog}.{schema}.drivers"
df_drivers=spark.table(bronze_drivers)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove the Underscore in email

# COMMAND ----------

df_drivers=df_drivers.withColumn("email",regexp_replace(col("email"),"_",""))

# COMMAND ----------

# MAGIC %md
# MAGIC Correct the formatof phone col

# COMMAND ----------

#remove other characters other than 0-9
df_drivers=df_drivers.withColumn("phone",regexp_replace(col("phone"),"[^0-9]",""))


# COMMAND ----------

#we want 10 digits if lower add 0 in front(total=11 digits include +1)
df_drivers=df_drivers.withColumn("phone",lpad(col("phone"),10,"0"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### we want in exact number format(american- +1-703-555-5XXX)

# COMMAND ----------

df_drivers =df_drivers.withColumn(
    "phone",
    expr("concat('+1-', substr(phone,1,3), '-', substr(phone,4,3), '-', substr(phone,7,4))")
)

# COMMAND ----------

display(df_drivers.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Menu table

# COMMAND ----------

bronze_menu=f"{catalog}.{schema}.menu"
df_menu=spark.table(bronze_menu)


# COMMAND ----------

# Count nulls per column
df_menu.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_menu.columns]).show()


# COMMAND ----------

display(df_menu.limit(100))

# COMMAND ----------

df_menu.printSchema()

# COMMAND ----------

df_menu=df_menu.withColumn("menu_id",col("menu_id").cast("long"))
df_menu=df_menu.withColumn("restaurant_id",col("restaurant_id").cast("long"))



# COMMAND ----------

df_menu.printSchema()

# COMMAND ----------

from pyspark.sql.types import DoubleType
df_menu=df_menu.withColumn("price",col("price").cast(DoubleType()))

# COMMAND ----------

display(df_menu.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### payments table

# COMMAND ----------

bronze_payments=f"{catalog}.{schema}.payments"
df_payments=spark.table(bronze_payments)

# COMMAND ----------

display(df_payments.limit(5))

# COMMAND ----------

#change timestamp
from pyspark.sql.functions import col, from_unixtime

df_payments = df_payments.withColumn(
    "payment_date",
    from_unixtime(col("payment_date").cast("double")).cast("timestamp")
)



# COMMAND ----------

display(df_payments.limit(5))

# COMMAND ----------

from pyspark.sql.types import DoubleType
df_payments=df_payments.withColumn("amount",col("amount").cast(DoubleType()))

# COMMAND ----------

df_payments=df_payments.withColumn("payment_id",col("payment_id").cast(DoubleType()))

# COMMAND ----------

df_payments.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ratings table

# COMMAND ----------

bronze_ratings=f"{catalog}.{schema}.ratings"
df_ratings=spark.table(bronze_ratings)

# COMMAND ----------

display(df_ratings.limit(5))

# COMMAND ----------

df_ratings.printSchema()

# COMMAND ----------

from pyspark.sql.types import FloatType
df_ratings=df_ratings.withColumn("rating_id",col("rating_id").cast(DoubleType()))
df_ratings=df_ratings.withColumn("order_id",col("order_id").cast(DoubleType()))
df_ratings=df_ratings.withColumn("rating",col("rating").cast(FloatType()))

# COMMAND ----------

df_ratings=df_ratings.withColumn("user_id",col("user_id").cast(DoubleType()))

# COMMAND ----------

df_ratings.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### restaurants

# COMMAND ----------

bronze_restaurants=f"{catalog}.{schema}.restaurants"
df_restaurants=spark.table(bronze_restaurants)

# COMMAND ----------

display(df_restaurants.limit(5))

# COMMAND ----------

df_restaurants=df_restaurants.drop("phone")

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, trim, col

df_restaurants = df_restaurants.withColumn(
    "address",
    trim(regexp_replace(col("address"), "\n", " "))
)


# COMMAND ----------

from pyspark.sql.functions import col

df_restaurants = df_restaurants.fillna({"address": "Unknown"})


# COMMAND ----------

display(df_restaurants.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### lOAD SILVER

# COMMAND ----------

def load_cleaned_to_delta(df_dict: dict, catalog_name: str, schema_name: str):
    """
    Load cleaned DataFrames into Delta tables.
    
    Parameters:
        df_dict (dict): Dictionary where key = table name, value = cleaned DataFrame.
        catalog_name (str): Target catalog name.
        schema_name (str): Target schema name.
    """
    
    if not df_dict:
        raise ValueError("No DataFrames provided to load.")

    for table_name, df in df_dict.items():
        if df is None:
            print(f"Skipping {table_name} (DataFrame is None)")
            continue

        print(f"Loading cleaned DataFrame into {catalog_name}.{schema_name}.{table_name}")
        
        # Write cleaned DF as Delta table
        df.write.format("delta").mode("overwrite").saveAsTable(
            f"{catalog_name}.{schema_name}.{table_name}"
        )


# COMMAND ----------

catalog_name = "datamodeling"
schema_name = "silver_cleaned"

# COMMAND ----------

cleaned_dfs = {
    "drivers": df_drivers,
    "restaurants": df_restaurants,
    "menu": df_menu,
    "ratings": df_ratings,
    "users": df_users,
    "orders": df_orders,
    "payments": df_payments,
    "address": df_address
}

load_cleaned_to_delta(cleaned_dfs,catalog_name, schema_name)


# COMMAND ----------

