#!/usr/bin/env python
# coding: utf-8

# #E-Commerce Customer and Products Modeling
# 
# Dim_Customer - RFM Analysis

# In[12]:


from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("E-Commerce Customer and Products Modeling")
    .enableHiveSupport()
    .getOrCreate()
)


# In[13]:


customers = spark.read.parquet(
    "/ecom_modeling/cleaned/customers_clean"
)

orders = spark.read.parquet(
    "/ecom_modeling/cleaned/orders_clean"
)

order_items = spark.read.parquet(
    "/ecom_modeling/cleaned/order_items_clean"
)

order_payments = spark.read.parquet(
    "/ecom_modeling/cleaned/order_payments_clean"
)


# In[16]:


customers.createOrReplaceTempView("customers")
orders.createOrReplaceTempView("orders")
order_items.createOrReplaceTempView("order_items")
order_payments.createOrReplaceTempView("order_payments")


# #cleaning order_payments

# In[39]:


duplicates = spark.sql("""
    SELECT
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value,
        COUNT(*) AS duplicate_count
    FROM order_payments
    GROUP BY
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value
    HAVING COUNT(*) > 1
""")

duplicates.show(20, truncate=False)


# In[48]:


order_payments_cleaned = spark.sql("""
    SELECT DISTINCT
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value
    FROM order_payments
""")

order_payments_cleaned.createOrReplaceTempView(
    "order_payments_cleaned"
)


# In[50]:


duplicates = spark.sql("""
    SELECT
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value,
        COUNT(*) AS duplicate_count
    FROM order_payments_cleaned
    GROUP BY
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value
    HAVING COUNT(*) > 1
""")

duplicates.show(20, truncate=False)


# #Filter Valid Orders & Identify Customers

# In[23]:


valid_customer_orders = spark.sql("""
    SELECT
        c.customer_unique_id,
        o.order_id,
        o.order_purchase_timestamp
    FROM customers c
    INNER JOIN orders o
        ON c.customer_id = o.customer_id
    WHERE o.order_status <> 'canceled'
""")

valid_customer_orders.show(5)


# In[25]:


valid_customer_orders.createOrReplaceTempView("valid_customer_orders")


# In[73]:


customer_payments = spark.sql("""
    SELECT
        v.customer_unique_id,
        v.order_id,
        v.order_purchase_timestamp,
        SUM(p.payment_value) AS order_total_spend
    FROM valid_customer_orders v
    INNER JOIN order_payments_cleaned p
        ON v.order_id = p.order_id
    GROUP BY
        v.customer_unique_id,
        v.order_id,
        v.order_purchase_timestamp
""")

customer_payments.show(10)


# In[74]:


customer_payments.createOrReplaceTempView("customer_payments")


# In[ ]:


#Calculate RFM Metrics


# In[75]:




rfm = spark.sql("""
    SELECT
        v.customer_unique_id,

        DATEDIFF(
            TO_DATE((SELECT MAX(order_purchase_timestamp)
                     FROM valid_customer_orders)),
            TO_DATE(MAX(v.order_purchase_timestamp))
        ) AS recency,

        COUNT(DISTINCT v.order_id) AS frequency,

        SUM(p.order_total_spend) AS monetary

    FROM valid_customer_orders v

    INNER JOIN customer_payments p
        ON v.order_id = p.order_id

    GROUP BY v.customer_unique_id

    HAVING SUM(p.order_total_spend) > 0
""")

rfm.show(10)


# In[77]:


rfm.createOrReplaceTempView("rfm")


# In[88]:


spark.sparkContext.setLogLevel("ERROR")


# In[89]:



rfm_scores = spark.sql("""
    SELECT
        customer_unique_id,
        recency,
        frequency,
        monetary,

        6 - NTILE(5) OVER (
            ORDER BY recency ASC
        ) AS recency_score,

        NTILE(5) OVER (
            ORDER BY frequency ASC
        ) AS frequency_score,

        NTILE(5) OVER (
            ORDER BY monetary ASC
        ) AS monetary_score

    FROM rfm
""")

rfm_scores.show(10)


# In[79]:


rfm_scores.createOrReplaceTempView("rfm_scores")


# In[86]:


spark.sparkContext.setLogLevel("ERROR")


# In[87]:



final_rfm = spark.sql("""
    SELECT
        customer_unique_id,
        recency,
        frequency,
        monetary,
        recency_score,
        frequency_score,
        monetary_score,

        CONCAT(
            recency_score,
            frequency_score,
            monetary_score
        ) AS RFM_SCORE

    FROM rfm_scores
""")

final_rfm.show(20, truncate=False)


# In[81]:


final_rfm.createOrReplaceTempView("final_rfm")


# In[100]:



customer_segments = spark.sql("""
    SELECT
        customer_unique_id,

        recency,
        frequency,
        monetary,

        recency_score,
        frequency_score,
        monetary_score,

        RFM_SCORE,

        CASE
            WHEN recency_score = 5
                 AND frequency_score BETWEEN 4 AND 5
                THEN 'Champions'

            WHEN recency_score BETWEEN 3 AND 4
                 AND frequency_score BETWEEN 4 AND 5
                THEN 'Loyal Customers'

            WHEN recency_score BETWEEN 4 AND 5
                 AND frequency_score BETWEEN 2 AND 3
                THEN 'Potential Loyalists'

            WHEN recency_score BETWEEN 1 AND 2
                 AND frequency_score BETWEEN 3 AND 4
                THEN 'At Risk'

            WHEN recency_score = 5
                 AND frequency_score = 1
                THEN 'New Customers'

            WHEN recency_score BETWEEN 1 AND 2
                 AND frequency_score BETWEEN 1 AND 2
                THEN 'Hibernating'

            ELSE 'Others'
        END AS Segment

    FROM final_rfm
""")

customer_segments.select(
    "customer_unique_id",
    "Segment",
    "recency_score",
    "frequency_score",
    "monetary_score",
     "RFM_SCORE"
).show(20, truncate=False)


# #Customer_DIM

# In[90]:


customer_details = spark.sql("""
    SELECT
        customer_unique_id,
        customer_city,
        customer_state
    FROM (
        SELECT
            customer_unique_id,
            customer_city,
            customer_state,
            ROW_NUMBER() OVER (
                PARTITION BY customer_unique_id
                ORDER BY customer_id
            ) AS rn
        FROM customers
    )
    WHERE rn = 1
""")

customer_details.show(10, truncate=False)


# In[96]:


customer_details.createOrReplaceTempView("customer_details")
customer_segments.createOrReplaceTempView("customer_segments")


# In[98]:


dim_customer = spark.sql("""
    SELECT
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,

        r.recency,
        r.frequency,
        r.monetary,

        r.recency_score,
        r.frequency_score,
        r.monetary_score,

        r.RFM_SCORE,
        r.Segment

    FROM customer_details c
    INNER JOIN customer_segments r
        ON c.customer_unique_id = r.customer_unique_id
""")

dim_customer.select(
    "customer_unique_id",
    "customer_city",
    "customer_state",
    "RFM_SCORE",
    "Segment"
).show(20, truncate=False)


# In[99]:


from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

dim_customer = dim_customer.withColumn(
    "cust_key",
    row_number().over(
        Window.orderBy("customer_unique_id")
    )
)

dim_customer = dim_customer.select(
    "cust_key",
    "customer_unique_id",
    "customer_city",
    "customer_state",
    "recency",
    "frequency",
    "monetary",
    "recency_score",
    "frequency_score",
    "monetary_score",
    "RFM_SCORE",
    "Segment"
)

dim_customer.select(
    "cust_key",
    "customer_unique_id",
    "customer_city",
    "customer_state",
    "RFM_SCORE",
    "Segment"
).show(20, truncate=False)


# In[101]:


dim_customer.write.mode("overwrite").parquet(
    "/ecom_modeling/output/dim_customer"
)


# In[102]:


order_payments_cleaned.write     .mode("overwrite")     .parquet("/ecom_modeling/output/order_payments_cleaned")


# In[ ]:




