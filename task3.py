from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, to_timestamp

# Initialize Spark session
spark = SparkSession.builder.appName("Task3_MovingAverage").getOrCreate()

# Set output paths
task3_output = "output/task3_moving_average.csv"
checkpoint_dir = "checkpoint/task3/"

# Read streaming data from the socket
stock_stream = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

def task3_moving_average(stock_stream):
    # TODO: Split the raw text data into columns and cast Timestamp
    # Hint: Use split function and to_timestamp for casting
    
    # TODO: Add a watermark to handle late data
    # Hint: Use withWatermark method and specify a delay (e.g., 1 minute)

    # TODO: Calculate moving average over a window of 15 seconds, sliding every 5 seconds
    # Hint: Use window function with groupBy and apply aggregation using avg function on Price column

    # Write the moving average to CSV (Complete mode for aggregation)
    # Uncomment the following lines after adding the logic for moving average calculation
    # moving_avg.writeStream.format("csv")\
    #     .option("path", task3_output)\
    #     .option("header", True)\
    #     .option("checkpointLocation", checkpoint_dir)\
    #     .outputMode("complete").start()\
    #     .awaitTermination()
    
    print(f"Task 3 output written to {task3_output}")

# Call the task function
task3_moving_average(stock_stream)
