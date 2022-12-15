package part1dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object DataFramesBasics extends App {
  // Creating a Spark Session
  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  // Reading a DF
  val firstDf = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  // Showing a DF
  firstDf.show()
  firstDf.printSchema()

  // Get rows
  firstDf.take(10).foreach(println)

  // Spark types
  val longType = LongType
  val integerType = IntegerType
  val stringType = StringType
  val doubleType = DoubleType
  val booleanType = BooleanType

  // Schema
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", IntegerType),
    StructField("Cylinders", IntegerType),
    StructField("Displacement", IntegerType),
    StructField("Horsepower", IntegerType),
    StructField("Weight_in_lbs", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  // Obtain a schema
  val carsDfSchema = firstDf.schema

  // Read DF with a defined schema
  val carsDfWithSchema = spark.read
    .format("json")
    .schema(carsSchema)
    .load("src/main/resources/data/cars.json")

  // Create rows
  // node: DFs have schemas, rows do not.
  val myRow = Row("Car Test", 18, 8, 307, 130, 3504, 12.0, "1970-01-01", "USA")

  // Create a DF from tuples
  val cars = Seq(
    ("Car Test", 18, 8, 307, 130, 3504, 12.0, "1970-01-01", "USA"),
    ("Car Test", 18, 8, 307, 130, 3504, 12.0, "1970-01-01", "USA"),
    ("Car Test", 18, 8, 307, 130, 3504, 12.0, "1970-01-01", "USA")
  )

  val manualCarsDf = spark.createDataFrame(cars) // schema auto inferred

  // Create DFs with implicits
  import spark.implicits._
  val manualCarsDfWithImplicits = cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "CountryOrigin")

  manualCarsDf.printSchema()
  manualCarsDfWithImplicits.printSchema()
}
