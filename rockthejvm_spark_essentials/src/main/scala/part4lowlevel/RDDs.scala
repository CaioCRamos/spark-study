package part4lowlevel

import org.apache.spark.sql.SparkSession

import scala.io.Source

object RDDs extends App {
  val spark = SparkSession.builder()
    .appName("Introduction do RDDs")
    .master("local[1]")
    .getOrCreate()

  // to access low level
  val sc = spark.sparkContext

  // 1. parallelize an existing collection
  val numbers = 1 to 10000
  val numbersRDD = sc.parallelize(numbers)

  // 2. reading from files
  case class StockValue(company: String, date: String, price: Double)

  def readStocks(filename: String) =
    Source.fromFile(filename)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  val stockRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2b. reading from files
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
    .map(line => line.split(","))
    .filter(tokens => tokens(0).toUpperCase() == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3. reading from a DF
  val stocksDF = spark.read
    .options(Map(
      "header" -> "true",
      "inferSchema" -> "true"
    ))
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._
  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd

  // RDD -> DF
  val numbersDF = numbersRDD.toDF("numbers") // you lose the type information

  // RDD -> DS
  val numbersDS = spark.createDataset(numbersRDD) // you keep the type information
}
