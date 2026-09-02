
# In[2]:


from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


# In[3]:
 

MYSQL_HOST = "localhost"
MYSQL_PORT = "3306"
MYSQL_DB = "olist_raw"
MYSQL_USER = "student"
MYSQL_PASSWORD = "student"

JDBC_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?useSSL=false&allowPublicKeyRetrieval=true&zeroDateTimeBehavior=convertToNull"

JDBC_PROPERTIES = {
    "user": MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "driver": "com.mysql.jdbc.Driver"
}


# In[8]:


spark = (
    SparkSession.builder
    .appName("Cleaning_DimProduct")
    .config("spark.jars", "/usr/local/spark3/spark-3.1.2-bin-hadoop3.2/jars/mysql-connector-java-5.1.47.jar")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# In[9]:


def read_table(table_name: str):
    
    return spark.read.jdbc(url=JDBC_URL, table=table_name, properties=JDBC_PROPERTIES)


# In[10]:


raw_customers = read_table("raw_customers")
raw_customers.show(5)


# In[27]:


raw_products = read_table("raw_products")
raw_orders = read_table("raw_orders")
raw_order_items = read_table("raw_order_items")
raw_order_payments = read_table("raw_order_payments")

print("")
for name, df in [
    ("customers", raw_customers),
    ("products", raw_products),
    ("orders", raw_orders),
    ("order_items", raw_order_items),
    ("order_payments", raw_order_payments),
]:
    print(f"{name}: {df.count()}")


# In[13]:


customers_clean = (
    raw_customers
    .dropDuplicates(["customer_id"])
    .filter(F.col("customer_id").isNotNull())
    .withColumn("customer_city", F.trim(F.lower(F.col("customer_city"))))
    .withColumn("customer_state", F.trim(F.upper(F.col("customer_state"))))
)

print(f"{raw_customers.count()}")
print(f"{customers_clean.count()}")
customers_clean.show(5)


# In[14]:


products_clean = (
    raw_products
    .dropDuplicates(["product_id"])
    .filter(F.col("product_id").isNotNull())
    .withColumn(
        "product_category_name",
        F.when(F.col("product_category_name").isNull(), "unknown")
         .otherwise(F.trim(F.lower(F.col("product_category_name"))))
    )
    .withColumn("product_weight_g",
                F.when(F.col("product_weight_g") > 0, F.col("product_weight_g")))
    .withColumn("product_length_cm",
                F.when(F.col("product_length_cm") > 0, F.col("product_length_cm")))
    .withColumn("product_height_cm",
                F.when(F.col("product_height_cm") > 0, F.col("product_height_cm")))
    .withColumn("product_width_cm",
                F.when(F.col("product_width_cm") > 0, F.col("product_width_cm")))
)

print(f"Before: {raw_products.count()}, After: {products_clean.count()}")
products_clean.show(5)


# In[28]:


orders_clean = (
    raw_orders
    .dropDuplicates(["order_id"])
    .filter(
        F.col("order_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("order_purchase_timestamp").isNotNull()  
    )
)


# In[16]:


order_items_clean = (
    raw_order_items
    .filter(
        F.col("order_id").isNotNull()
        & F.col("product_id").isNotNull()
        & F.col("price").isNotNull()
        & (F.col("price") > 0)          
    )
    .dropDuplicates(["order_id", "order_item_id"])
)


# In[17]:


order_payments_clean = (
    raw_order_payments
    .filter(
        F.col("order_id").isNotNull()
        & F.col("payment_value").isNotNull()
        & (F.col("payment_value") >= 0)
    )
)


# In[18]:


print("\n")
for name, df in [
    ("customers_clean", customers_clean),
    ("products_clean", products_clean),
    ("orders_clean", orders_clean),
    ("order_items_clean", order_items_clean),
    ("order_payments_clean", order_payments_clean),
]:
    print(f"{name}: {df.count()}")


# In[19]:


price_history = (
    order_items_clean
    .join(orders_clean.select("order_id", "order_purchase_timestamp"), on="order_id", how="inner")
    .select(
        "product_id",
        F.col("order_purchase_timestamp").cast("date").alias("price_date"),
        "price"
    )
)


# In[20]:


price_history_dedup= price_history.dropDuplicates(["product_id", "price_date", "price"])


# In[21]:


window_by_product_date = Window.partitionBy("product_id").orderBy("price_date")

price_with_prev = price_history_dedup.withColumn(
    "prev_price", F.lag("price").over(window_by_product_date)
)


# In[22]:


price_changes = price_with_prev.filter(
    F.col("prev_price").isNull() | (F.col("price") != F.col("prev_price"))
).drop("prev_price")


# In[23]:


window_next_change = Window.partitionBy("product_id").orderBy("price_date")

product_price_scd = (
    price_changes
    .withColumn("dw_start_date", F.col("price_date"))
    .withColumn(
        "next_start_date",
        F.lead("price_date").over(window_next_change)
    )
    .withColumn(
        "dw_end_date",
        F.when(F.col("next_start_date").isNotNull(), F.date_sub(F.col("next_start_date"), 1))
         .otherwise(F.lit("9999-12-31").cast("date"))
    )
    .select("product_id", F.col("price").alias("product_price"), "dw_start_date", "dw_end_date")
)


# In[24]:


dim_product = (
    product_price_scd
    .join(products_clean, on="product_id", how="left")
    .withColumn("product_key", F.monotonically_increasing_id())  # Surrogate Key
    .select(
        "product_key",
        "product_id",
        "product_category_name",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "product_price",
        "dw_start_date",
        "dw_end_date",
    )
    .orderBy("product_id", "dw_start_date")
)

print("\n")
dim_product.show(10, truncate=False)
print(f"Number of rows of Dim_Product: {dim_product.count()}")
print(f"Number of unique products: {dim_product.select('product_id').distinct().count()}")


# In[33]:


customers_clean.write.mode("overwrite").parquet(
    "/user/student/cleaned_data/customers_clean"
)

orders_clean.write.mode("overwrite").parquet(
    "/user/student/cleaned_data/orders_clean"
)

order_items_clean.write.mode("overwrite").parquet(
    "/user/student/cleaned_data/order_items_clean"
)

order_payments_clean.write.mode("overwrite").parquet(
    "/user/student/cleaned_data/order_payments_clean"
)

products_clean.write.mode("overwrite").parquet(
    "/user/student/cleaned_data/products_clean"
)

dim_product.write.mode("overwrite").parquet(
    "/user/student/cleaned_data/dim_product"
)

# In[34]:


spark.read.parquet(f"/user/student/cleaned_data/customers_clean").show(5)

