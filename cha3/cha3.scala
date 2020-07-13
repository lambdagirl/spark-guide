import spark.implicits._
val staticDataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("./data/retail-data/by-day/*.csv")
staticDataFrame.createOrReplaceTempView("retail_data")
val staticSchema = staticDataFrame.schema

import org.apache.spark.sql.functions.{window, column, desc, col}
staticDataFrame.selectExpr("customerID", "(UnitPrice * Quantity) as total_cost", "InvoiceDate").groupBy(
        col("CustomerID"), window(col("InvoiceDate"), "1 day")).sum("total_cost").show()

// spark.conf.set("spark.sql.shuffle.partitions", "5")

// COMMAND Spark Streaming
val streamingDataFrame = spark.readStream.schema(staticSchema).option("maxFilesPerTrigger", 1).format("csv").option("header","true").load("./data/retail-data/by-day/*.csv")

// streamingDataFrame.isStreaming

val purchaseByCustomerPerHour = streamingDataFrame.selectExpr("customerID", "(UnitPrice * Quantity) as total_cost", "InvoiceDate").groupBy(
    $"CustomerID", window($"InvoiceDate","1 day")).sum("total_cost")

purchaseByCustomerPerHour.writeStream.format("memory").queryName("customer_purchases").outputMode("complete").start()


//Machine Learning and Advanced Analytics
staticDataFrame.printSchema()
import org.apache.spark.sql.functions.date_format
val preppedDataFrame = staticDataFrame.na.fill(0).withColumn("day_of_week", date_format($"InvoiceDate", "EEEE")).coalesce(5)

//split the data into training and test sets
val trainDataFrame = preppedDataFrame.where("InvoiceDate < '2011-07-01' ")
val testDataFrame = preppedDataFrame.where("InvoiceData >= '2011-07-01' ")