package part2typesdatasets

import org.apache.spark.sql.functions.array_contains
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

object Datasets extends App {
  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF = spark.read
    .format("csv")
    .options(Map(
      "header" -> "true",
      "inferSchema" -> "true"
    ))
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

  // Every Dataframe is Dataset[Row]

  // convert a Dataframe to a Dataset
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  // dataset of a complex type
  // 1 - define your case class
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  //Year: Date,
                  Origin: String
                )

  // 2 - read the DF from the file
  def readDF(filename: String): DataFrame =
    spark.read.option("inferSchema", "true").json(s"src/main/resources/data/$filename")

  // 3 - define an encoder (importing the implicits)

  import spark.implicits._

  val carsDF = readDF("cars.json")

  // 4 - convert DF to DS
  val carsDS = carsDF.as[Car]

  // DS collection funtions
  numbersDS.filter(_ < 100).show()

  // map, flatMap, fold, reduce, for comprehensions ...
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())
  carNamesDS.show()

  // Joins
  case class Guitar(id: Long, make: String, model: String, guitarType: String)

  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)

  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  guitarPlayersDS
    .joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")
    .show()

  guitarPlayersDS
    .joinWith(guitarsDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")
    .show()

  // Grouping DS
  carsDS
    .groupByKey(_.Origin)
    .count()
    .show()

  // joins and groups are WIDE transformations, will involve SHUFFLE operations
}
