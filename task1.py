from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("Task1_FilteredStream").getOrCreate()

# Set output paths
task1_output = "output/task1_filtered_stream.csv"
checkpoint_dir = "checkpoint/task1/"

# Read streaming data from the socket
stock_stream = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

def task1_filtered_ingestion(stock_stream):
    # TODO: Split the raw text data into columns (Timestamp, StockSymbol, Price)
    # Hint: Use split function and selectExpr to split the stream into respective columns
    
    # TODO: Filter for stocks whose price is greater than a certain value (e.g., 100)
    # Hint: Use filter or where method to apply conditions based on Price column

    # Write the filtered stream to CSV (Append mode since no aggregation)
    # Uncomment the following lines after adding the logic for filtering the stream
    # filtered_stream.writeStream.format("csv")\
    #     .option("path", task1_output)\
    #     .option("header", True)\
    #     .option("checkpointLocation", checkpoint_dir)\
    #     .outputMode("append").start()\
    #     .awaitTermination()
    
    print(f"Task 1 output written to {task1_output}")

# Call the task function
task1_filtered_ingestion(stock_stream)
