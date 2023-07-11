#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark

findspark.init()


# In[3]:


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Datacamp Pyspark Tutorial").config("spark.memory.offHeap.enabled","true").config("spark.memory.offHeap.size","10g").getOrCreate()


# In[4]:


df = spark.read.csv('online_retail.csv',header=True,escape="\"")


# In[5]:


df.show(5,0)


# In[6]:


df.count()


# #### How many unique customers are present in the dataframe?

# In[7]:


df.select('CustomerID').distinct().count()


# #### What country do most purchases come from?

# In[8]:


from pyspark.sql.functions import *
from pyspark.sql.types import *

df.groupBy('Country').agg(countDistinct('CustomerID').alias('country_count')).show()


# #### ordered

# In[9]:


df.groupBy('Country').agg(countDistinct('CustomerID').alias('country_count')).orderBy(desc('country_count')).show()


# #### When was the most recent purchase made by a customer on the e-commerce platform?

# In[15]:


spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

df = df.withColumn('date',to_timestamp("InvoiceDate", 'MM/dd/yy HH:mm'))
df.select(max("date")).show()


# #### When was the earliest purchase made by a customer on the e-commerce platform?

# In[16]:


df.select(min("date")).show()


# ## Data Pre-processing

# In[19]:


df.show(3,0)


# In[20]:


df = df.withColumn("from_date", lit("12/1/10 08:26"))
df = df.withColumn('from_date',to_timestamp("from_date", 'MM/dd/yy HH:mm'))

df2=df.withColumn('from_date',to_timestamp(col('from_date'))).withColumn('recency',col("date").cast("long") - col('from_date').cast("long"))


# In[21]:


df.show(2,0)


# In[22]:


df2.show(2,0)


# In[23]:


# Read CSV file into table
df = spark.read.option("header",True).csv("online_retail.csv")
df.printSchema()
df.show()


# In[26]:


# Read CSV file into table
spark.read.option("header",True).csv("online_retail.csv").createOrReplaceTempView("ECOMMERCE")


# In[30]:


# SQL Select query
spark.sql("SELECT invoiceno, description, quantity, customerid, country FROM ECOMMERCE order by customerid").show()


# In[ ]:




