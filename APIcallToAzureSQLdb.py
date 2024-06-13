#!/usr/bin/env python
# coding: utf-8

# ## APIcallToAzureSQLdb
# Developer - Nosa


import requests
import numpy
import pandas as pd
import datetime as datetime
import os.path

base_url = "https://api.openweathermap.org/data/2.5/weather"

params = {
    "q": "London",  
    "appid": "7726b0b5d091e3319b9ca19f8a1dbbc8"  
}

response = requests.get(base_url, params=params)

if response.status_code == 200:
    
    data = response.json()
    
    
    print(data)
else:
    print(f"Failed to retrieve weather data: {response.status_code}")


# In[2]:


df = pd.DataFrame([data])
df


# In[20]:


import requests
import numpy
import pandas as pd
import datetime as datetime
import os.path

# Code 1: Fetch weather data from OpenWeatherMap API
base_url = "https://api.openweathermap.org/data/2.5/weather"
params = {
    "q": "Michigan",  
    "appid": "7726b0b5d091e3319b9ca19f8a1dbbc8"  
}

response = requests.get(base_url, params=params)

if response.status_code == 200:
    data = response.json()
    print("Weather data retrieved successfully.")
else:
    print(f"Failed to retrieve weather data: {response.status_code}")
    data = {}


# In[21]:


#Code 2: Write data to Azure SQL Database

# Ensure the data is in a dictionary format to convert to a pandas DataFrame
# Extracting relevant data for simplicity
weather_data = {
   "weather_id": data.get("weather", [{}])[0].get("id"),
   "city": data.get("name"),
   "temperature": data.get("main", {}).get("temp"),
   "humidity": data.get("main", {}).get("humidity"),
   "weather": data.get("weather", [{}])[0].get("description")

}

# Create a pandas DataFrame
df = pd.DataFrame([weather_data])


# In[22]:


from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AzureSQLNewTableWrite") \
    .getOrCreate()

# Prepare the JDBC URL with credentials to DB
jdbc_url = (
    "jdbc:sqlserver://nosa-practice.database.windows.net;"  # server name
    "databaseName=;"
    "user=nosa;"
    "password=123;"
)

# Convert pandas DataFrame to Spark DataFrame
spark_df = spark.createDataFrame(df)


# In[23]:


# Define the new table name to write to
new_table_name = "tbl_NewWeatherForecast"

# Write the Spark DataFrame to the new Azure SQL Database table
spark_df.write \
    .format("jdbc") \
    .mode("append") \
    .option("url", jdbc_url) \
    .option("dbtable", new_table_name) \
    .option("user", "nosa") \
    .option("password", "123") \
    .save()

print(f"DataFrame written to new Azure SQL Database table: {new_table_name}")

