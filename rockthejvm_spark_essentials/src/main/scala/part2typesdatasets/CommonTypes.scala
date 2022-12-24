package part2typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App {
  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  // Adding a plain value to a DF
  moviesDF.select(col("Title"), lit(47).as("plain_value")).show()

  // Booleans
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter

  // filter
  moviesDF.select("Title").where(preferredFilter)

  // add column and filter
  val moviesWithNewColumnDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))
  moviesWithNewColumnDF.where("good_movie") // where(col("good_movie") === "true")

  // negations
  moviesWithNewColumnDF.where(not(col("good_movie")))

  // Numbers
  // math operators
  moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)

  // correlation = number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")) // corr is an ACTION

  // Strings
  val carsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/cars.json")

  // capitalization: initcap, lower, upper
  carsDF.select(initcap(col("Name"))).show()

  // contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))

  // regex
  val regexString = "volkswagen|vw"
  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  ).where(col("regex_extract") =!= "").show()

  /*
    Exercise:
    Filter the carsDF by a list of car names obtained by an API call
   */

  def getCarNames(): List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

  // version 1 - regex
  val complexRegex = getCarNames().map(_.toLowerCase()).mkString("|")
  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), complexRegex, 0).as("regex_extract"),
  ).where(col("regex_extract") =!= "").show()

  // version 2 - contains
  val carNameFilters = getCarNames().map(_.toLowerCase()).map(name => col("Name").contains(name))
  val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
  carsDF.filter(bigFilter).select("Name").show()
}
