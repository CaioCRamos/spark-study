package part4lowlevel

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}

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

  def readStocks(filename: String): Seq[StockValue] =
    Source.fromFile(filename)
      .getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

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

  // Transformations
  val msftRDD = stocksRDD.filter(_.company == "MSFT") // lazy transformation

  // counting
  val msftCount = msftRDD.count() // eager action

  // distinct
  val companyNames = stocksRDD.map(_.company).distinct() // lazy

  // min and max
  implicit val stockOrdering: Ordering[StockValue] = Ordering.fromLessThan((sa, sb) => sa.price < sb.price)
  val minMsft = msftRDD.min() // action

  // reduce
  numbersRDD.reduce(_ + _)

  // grouping
  val groupedStocksRDD = stocksRDD.groupBy(_.company) // very expensive, involves shuffling

  // Partitioning

  val repartitionedStocksRDD = stocksRDD.repartition(30)
  repartitionedStocksRDD.toDF
    .write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stock30")
  /*
    Repartitioning is EXPENSIVE. Involves Shuffling.
    Best practice: partition EARLY, then process that.
    Size of a partition: 10-100MB
  */

  // coalesce
  val coalescedStocksRDD = repartitionedStocksRDD.coalesce(15) // does NOT involves shuffling
  coalescedStocksRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks15")

  /**
   * Exercises
   *
   * 1. Read the movies.json as an RDD.
   * 2. Show the distinct genres as an RDD.
   * 3. Select all the movies in the Drama genre with IMDB rating > 6.
   * 4. Show the average rating of movies by genre.
   */

  case class Movie(title: String, genre: String, rating: Double)

  // 1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val moviesRDD = moviesDF
    .select(col("Title").as("title"), col("Major_Genre").as("genre"), col("IMDB_Rating").as("rating"))
    .where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd

  // 2
  val genresRDD = moviesRDD.map(_.genre).distinct()

  // 3
  val goodDramasRDD = moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6)

  // 4
  case class GenreAvgRating(genre: String, rating: Double)

  val avgRatingByGenreRDD = moviesRDD.groupBy(_.genre).map {
    case (genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
  }

  avgRatingByGenreRDD.toDF.show
  moviesRDD.toDF.groupBy(col("genre")).avg("rating").show
}

