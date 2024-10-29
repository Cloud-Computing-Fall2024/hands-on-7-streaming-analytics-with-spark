from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("Task2_CurrencyConversion").getOrCreate()

# Set output paths
task2_output = "output/task2_currency_conversion.csv"
checkpoint_dir = "checkpoint/task2/"

# Read streaming data from the socket
stock_stream = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

def task2_currency_conversion(stock_stream):
    conversion_rate = 0.85  # Example conversion rate from USD to EUR
    
    # TODO: Split the raw text data into columns (Timestamp, StockSymbol, Price)
    # Hint: Use split function and selectExpr to split the stream into respective columns

    # TODO: Convert the Price to EUR
    # Hint: Use withColumn to create a new column called "PriceEUR" and multiply the Price by the conversion rate

    # Write the converted prices to CSV
    # Uncomment the following lines after adding the logic for currency conversion
    # stock_stream_converted.writeStream.format("csv")\
    #     .option("path", task2_output)\
    #     .option("header", True)\
    #     .option("checkpointLocation", checkpoint_dir)\
    #     .outputMode("append").start()\
    #     .awaitTermination()
    
    print(f"Task 2 output written to {task2_output}")

# Call the task function
task2_currency_conversion(stock_stream)
