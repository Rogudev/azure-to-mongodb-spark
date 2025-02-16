# Mongo DB Uploader using Databricks and Spark

## Overview

This project provides a real-time streaming solution using Apache Spark on Databricks for uploading exercise-related data into MongoDB. The data, which comes from CSV files stored in Azure Blob Storage, is processed in batches and uploaded to MongoDB collections based on the exercise type.

The procesment can be tested with `test_procesment.py`. This script would takes `workout_data.csv`, located in the `1-input/` directory, and separates it into different CSV files classified by exercise name, saving them in the `2-output/` directory.

Additionally, the script generates a list of MongoDB collection names based on the exercise names, formatted in the main directory.

MongoDB collections can be created using `create_collections.py`. This script connects to MongoDB, uses the previously generated list of exercises, and creates the corresponding collections in the database.

## Key Features:
- Real-time data streaming and batch processing.
- Automatic transformation of input data into the appropriate format for MongoDB.
- Handles missing values and processes columns to clean the dataset.
- Utilizes Azure Blob Storage and MongoDB for data storage.
- CSV schema definition to handle structured data.


## Requirements

The following packages are required to run the project:

```bash
!pip install pandas
!pip install unidecode
```


## Libraries Used

- **pyspark**: For setting up the Spark session, reading CSV streams, and performing transformations.
- **pandas**: For data processing and manipulation in Python.
- **unidecode**: To remove special characters for MongoDB collection naming.
- **functions**: For Spark SQL functions used for data transformations.


## Project Flow

### 1. Spark Session Initialization
A Spark session is initialized with the app name "Mongo DB Uploader". The session handles all the operations related to reading the CSV file, processing data, and uploading the transformed data to MongoDB.

```python
spark = SparkSession.builder.appName("Mongo DB Uploader").getOrCreate()
```

### 2. CSV Schema Definition
The schema for the input CSV is defined using Spark's StructType and StructField. This ensures that the data is read in a structured way and each field in the CSV is typed correctly.

```python
csv_Scheme = StructType([
    StructField("title", StringType(), True),
    StructField("start_time", StringType(), True),
    ...
])
```

### 3. Cloud Storage Configuration
The Azure Blob Storage configuration is set by providing the storage account name, account key, and container name. This allows the Spark session to access the input data stored in Azure Blob Storage.

```python
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", storage_account_key)
csvPath = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/input/"
```

### 4. Data Processing Functions
Several helper functions are defined to:

- **Drop Unwanted Columns**: The `drop_cols` function removes unused columns.
- **Fragment Data**: The `fragment_df` function splits the DataFrame into multiple smaller DataFrames based on exercise types.
- **Process NaN Values**: The `process_na_column` function handles missing values by using forward/backward fill or averaging neighboring values.
- **Replace Spanish Month Names**: The `replace_months_es_to_en` function replaces Spanish month names with their English equivalents.

### 5. Data Testing (Local Processing)
For local testing and debugging, the `test_processing.py` file can be used to check how the data is processed before running the script in Databricks. This file processes the input CSV file and verifies that the necessary transformations (such as removing unwanted columns and handling missing values) are performed correctly.

```python
import pandas as pd
import locale
from unidecode import unidecode

# set locale for es, that would prevent the date error, bc the months are in Spanish
locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')

# Functions to process the data...
```
Run the script locally to verify the processing steps:
```python
python test_processing.py
```
### 6. MongoDB Collections Setup (`create_collections.py`)
Before uploading data to MongoDB, the necessary collections must be created. The `create_collections.py` script reads the list of exercises and creates corresponding collections in MongoDB. This file ensures that each exercise (e.g., "running", "cycling") has a dedicated collection in your MongoDB database.

```python
from pymongo import MongoClient

# connect to mongo atlas
uri = '<YOUR_MONGO_URI>'
conn = MongoClient(uri)

# select database
db = conn.gym_tracker

# load the collections list
exercises_list = []
with open('exercises_list.txt', 'r') as file:
    exercises_list = [line.strip() for line in file.readlines()]

# create collections
for exercise in exercises_list:
    db.create_collection(exercise)
```

### 7. MongoDB Writing Logic
The write_row function processes the DataFrame and uploads it to MongoDB:

It drops unnecessary columns and filters out rows with missing exercise titles.
The data is then split by exercise type and processed for missing values and date formatting.
The cleaned data is uploaded to different MongoDB collections based on the exercise name, ensuring that the data is properly categorized.

```python
exercises_df_list[i] = process_na_column(exercises_df_list[i], scheme_cols)
exercises_df_list[i]['start_time'] = pd.to_datetime(exercises_df_list[i]['start_time'], format='%d %b %Y, %H:%M')
```
The function uploads each processed exercise's data to the corresponding MongoDB collection by converting the DataFrame into Spark DataFrame format and then writing it to MongoDB:
```python
spark_df.write.format("mongodb").mode("append") \
    .option("connection.uri", "<YOUR_MONGO_URI>")\
    .option("database", "<YOUR_DATABASE>") \
    .option("collection", f"{collection}") \
    .save()
```

### 8. Streaming Data Processing
The project uses Spark Streaming to continuously read new CSV files from the Azure Blob Storage and process them in small batches. The data is processed using the defined transformations, and results are written to MongoDB.

```python
df = spark.readStream \
    .schema(csv_Scheme) \
    .option('header', 'true') \
    .option('maxFilesPerTrigger', 1) \
    .csv(csvPath)
```
The following method is used to write the processed data to MongoDB:
```python
df.writeStream \
    .foreachBatch(write_row) \
    .option("checkpointLocation", checkpoint_path) \
    .start() \
    .awaitTermination()
```

### 9. Checkpoints
To avoid reprocessing the same files, checkpoints are used to track the streaming state. This ensures that only new files are processed in each batch. The checkpoint path is specified in the following way:
```python
checkpoint_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/checkpoints/"
```

### 10. MongoDB Connection
The MongoDB connection URI, database name, and collection names are specified in the write_row function. Data is appended to the corresponding MongoDB collection based on the exercise type. This allows the data to be categorized into collections corresponding to different exercises.
```python
spark_df.write.format("mongodb").mode("append") \
    .option("connection.uri", "<YOUR_MONGO_URI>")\
    .option("database", "<YOUR_DATABASE>") \
    .option("collection", f"{collection}") \
    .save()
```

## Configurations
Replace the following placeholders with your actual values:

YOUR_ACCOUNT_NAME: Your Azure Blob Storage account name.
YOUR_ACCOUNT_KEY: Your Azure Blob Storage account key.
YOUR_CONTAINER_NAME: The name of your Azure Blob Storage container.
YOUR_MONGO_URI: The connection URI for your MongoDB instance.
YOUR_DATABASE: The name of the MongoDB database where data will be uploaded.

### Schemes