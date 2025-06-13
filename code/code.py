# Databricks notebook source
from pyspark.sql.window import *

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
schema = StructType([
    StructField("Row ID", IntegerType(), True),
    StructField("Order ID", StringType(), True),
    StructField("Order Date", StringType(), True),
    StructField("Ship Date", StringType(), True),
    StructField("Ship Mode", StringType(), True),
    StructField("Customer ID", StringType(), True),
    StructField("Customer Name", StringType(), True),
    StructField("Segment", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Postal Code", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Product ID", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Sub-Category", StringType(), True),
    StructField("Product Name", StringType(), True),
    StructField("Sales", DoubleType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("Profit", DoubleType(), True)
])
df=spark.read.schema(schema).format('csv').option('header',True).load('/FileStore/tables/Sample___Superstore-1.csv')

# COMMAND ----------

df=df.withColumn('Order Date',trim(regexp_replace('Order Date','[^0-9-/]','')))

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

df=df.withColumn('Order Date',coalesce(
    to_date('Order Date','MM-dd-yyyy'),
    to_date('Order Date','dd-MM-yyyy'),
    to_date('Order Date','MM/dd/yyyy'),
    to_date('Order Date','M/d/yyyy')
))

# COMMAND ----------

df=df.withColumn('Ship Date',trim(regexp_replace('Ship Date','[^0-9-/]','')))

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

df=df.withColumn('Ship Date',coalesce(
    to_date('Ship Date','MM-dd-yyyy'),
    to_date('Ship Date','dd-MM-yyyy'),
    to_date('Ship Date','MM/dd/yyyy'),
    to_date('Ship Date','M/d/yyyy')
))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Extract Order Year, Order Month, and Order Weekday from the Order Date

# COMMAND ----------

df=df.withColumn('Order Year',year('Order Date'))\
    .withColumn('Order Month',month('Order Date'))\
        .withColumn('Order Weekday',dayofweek('Order Date'))\
            .withColumn('Order Day',dayofmonth('Order Date'))

# COMMAND ----------

df = df.withColumn('Order Weekday Name', date_format('Order Date', 'EEEE'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Profit Margin %

# COMMAND ----------

df=df.withColumn('Proit_Margin_percentage',(col('Profit')/col('Sales') ) *100)

# COMMAND ----------

df=df.withColumn('Product Name',regexp_replace('Product Name','[^a-zA-Z0-9 ]' , '' ))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Is_Profitable

# COMMAND ----------

df= df.withColumn('Is_Profitable', when(col('Profit') > 0, 'Yes').otherwise('No'))


# COMMAND ----------

# MAGIC %md
# MAGIC ###High_Discount_Flag

# COMMAND ----------

df=df.withColumn('High_Discount_Flag',when(col('Discount') > 0.2,'Yes').otherwise('No')
                 )

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sales by Region and Category
# MAGIC
# MAGIC

# COMMAND ----------

sales_by_region_category=df.groupBy('Region').pivot('Category').agg(sum('Sales'))

# COMMAND ----------

sales_by_region_category.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Customer Lifetime Value 

# COMMAND ----------

Customer_Lifetime_Value=df.groupBy('Customer ID')\
    .agg(sum('Sales').alias('Total Sales')
         ,sum('Profit').alias('Total profit')
         ,countDistinct('Order ID').alias('Number of orders')
         ).orderBy('Total Sales', ascending=False)

# COMMAND ----------

Customer_Lifetime_Value=Customer_Lifetime_Value.orderBy(desc('Total Sales'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Customer Lifetime Value ,is profitable or not

# COMMAND ----------

Customer_Lifetime_Value=Customer_Lifetime_Value.withColumn('Profitability',
                                                           when(col('Total profit')>0,'Yes').otherwise('No')
                                                           )

# COMMAND ----------

# MAGIC %md
# MAGIC ###State of performance

# COMMAND ----------

windowSpec = Window.orderBy(desc('Total profit'))

Customer_Lifetime_Value = Customer_Lifetime_Value.withColumn(
    'Rank',
    rank().over(windowSpec)
)

# COMMAND ----------

Customer_Lifetime_Value.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Frequent Buyers
# MAGIC
# MAGIC

# COMMAND ----------

Frequent_Buyers = Customer_Lifetime_Value.withColumn(
    'Frequent Buyer',
    when(col('Number of orders') > 5, 'Yes').otherwise('No')
)

# COMMAND ----------

Frequent_Buyers.display()

# COMMAND ----------

sales_by_category = df.groupBy('Category').agg(round(sum('Sales'),2).alias('Total Sales'))

display(sales_by_category)

# COMMAND ----------

import matplotlib.pyplot as plt

# Convert to Pandas DataFrame for plotting
pdf = sales_by_category.toPandas()

plt.figure(figsize=(6,6))
plt.pie(pdf['Total Sales'], labels=pdf['Category'], autopct='%1.1f%%', startangle=140)
plt.title('Sales by Category')
plt.show()
