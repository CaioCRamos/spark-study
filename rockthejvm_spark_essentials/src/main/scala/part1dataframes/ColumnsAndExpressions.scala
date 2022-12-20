package part1dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App{
  val spark = SparkSession.builder()
    .appName("DF Columns And Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  var carsDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDf.show()

  // Columns
  var nameColumn = carsDf.col("name")

  // Selecting (projecting)
  carsDf.select(nameColumn)

  // Various select methods
  import spark.implicits._

  carsDf.select(
    col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    //"Year", // Scala symbol, auto converted to Column
    $"Horsepower", // Fancier interpolated string, returns a Column object
    expr("Origin") // Expression
  )

  // Selecting with plain column types
  carsDf.select("Name", "Year")

  // Expressions
  val weightInKgExpression = col("Weight_in_lbs") / 2.2
  val carsWithWeightsDf = carsDf.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  // SelectExpr
  var carsWithSelectExprWeightsDf = carsDf.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF processing
  // Adding a column
  val carsWithKg = carsDf.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  // Renaming a column
  val carsWithColumnRenamed = carsDf.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // Careful with columns names
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")
  // Remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // Filtering
  val notUSACars = carsDf.filter(col("Origin") =!= "USA") // =!= different / === equal
  val notUSACars2 = carsDf.where(col("Origin") =!= "USA")

  // Filtering with expression strings
  val americanCarsDf = carsDf.filter("Origin = 'USA'")

  // Chain filter
  val americanPowerfulCarsDf = carsDf.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDf2 = carsDf.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDf3 = carsDf.filter("Origin = 'USA' and Horsepower > 150")

  // Union = adding more rows
  val moreCarsDf = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/more_cars.json")

  val allCarsDf = carsDf.union(moreCarsDf) // works if the DFs have the same schema

  // Distinct values
  val allCountriesDf = carsDf.select("Origin").distinct()
}
