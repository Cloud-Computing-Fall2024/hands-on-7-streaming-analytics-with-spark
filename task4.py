from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, max, min, expr, to_timestamp

# Initialize Spark session
spark = SparkSession.builder.appName("Task4_PriceSpikes").getOrCreate()

# Set output paths
task4_output = "output/task4_price_spike_alerts.csv"
checkpoint_dir = "checkpoint/task4/"

# Read streaming data from the socket
stock_stream = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

def task4_price_spikes(stock_stream):
    # TODO: Split the raw text data into columns and cast Timestamp
    # Hint: Use split function and to_timestamp for casting
    
    # TODO: Add a watermark to handle late data
    # Hint: Use withWatermark method and specify a delay (e.g., 1 minute)

    # TODO: Calculate max and min prices over a window of 15 seconds, sliding every 5 seconds
    # Hint: Use window function with groupBy and apply aggregation using max and min functions on Price column

    # TODO: Detect spikes where price changes by more than 10%
    # Hint: Filter rows where MaxPrice is greater than MinPrice * 1.1

    # Write the detected spikes to CSV (Complete mode for aggregation)
    # Uncomment the following lines after adding the logic for price spike detection
    # price_spikes_flattened.writeStream.format("csv")\
    #     .option("path", task4_output)\
    #     .option("header", True)\
    #     .option("checkpointLocation", checkpoint_dir)\
    #     .outputMode("complete").start()\
    #     .awaitTermination()
    
    print(f"Task 4 output written to {task4_output}")

# Call the task function
task4_price_spikes(stock_stream)
