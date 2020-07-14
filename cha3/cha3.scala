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
val testDataFrame = preppedDataFrame.where("InvoiceDate >= '2011-07-01'")

//these transformations are DataFrame transformations
import org.apache.spark.ml.feature.StringIndexer
val indexer = new StringIndexer().setInputCol("day_of_week").setOutputCol("day_of_week_index")
//this will trurn our days of weeks into corresponding numerical value. Spark might represent Saturday as 6 and Monday as 1.
//Saturday is not greater than Monday, use OneHotEncoder to encode each of these values into their own column.
import org.apache.spark.ml.feature.OneHotEncoder
val encoder = new OneHotEncoder().setInputCol("day_of_week_index").setOutputCol("day_of_week_encoded")

//Assemble columns into a vector.
import org.apache.spark.ml.feature.VectorAssembler()
val vectorAssembler = new VectorAssembler().setInputCols(Array("UnitPrice","Quantity","day_of_week_encoded")).setOutputCol("features")

//set this up into a pipeline, so any future data we need to transform can go through the exact same process
import org.apache.spark.ml.Pipeline

val transformationPipeline = new Pipeline().setStages(Array(indexer, encoder, vectorAssembler))

//Two step process to preparing for training process. 
//1.fit our transformers to this dataset. ( StringIndexer needs to know how many unique values there are to be indexed. After those exist, encoding is easy but Spark must look at all the distinct values in the column to be indexed in order to store those values later on)
val fittedPipeline = transformationPipeline.fit(trainDataFrame)
//2.use fitted pipline to transform all of data in a consistent and repeatable way.
val transformedTraining = fittedPipeline.transform(trainDataFrame)

transformedTraining.cache()

//We now have a training set; it’s time to train the model. First we’ll import the relevant model that we’d like to use and instantiate it
import org.apache.spark.ml.clustering.KMeans
val kmeans = new KMeans().setK(20).setSeed(1L)

//There are always two types for every algorithm in MLlib’s DataFrame API. They follow the naming pattern of Algorithm, for the untrained version, and AlgorithmModel for the trained version. In our example, this is KMeans and then KMeansModel.
val kmModel = kmeans.fit(transformedTraining)
kmModel.computeCost(transformedTraining)

val transformedTest - fittedPipline.transform*testDataFrame)
kmModel.computeCost(transformedTest)