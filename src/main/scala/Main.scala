import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

@main def hello(): Unit =
  println("Hello world!")

  val spark = SparkSession
    .builder()
    .appName("Spark SQL Learner")
    .master(sys.env.getOrElse("SPARK_MASTER_URL", "local[*]"))
    .getOrCreate()
    
  spark.sparkContext.setLogLevel("WARN")

  val path = ".\\src\\population_2024.csv"

  var df = spark
    .read.option("header", "true")
    .csv(path)
  
  df = df
    .withColumn("Rank", df("Rank").cast(IntegerType))
    .withColumn("Population 2024", df("Population 2024").cast(IntegerType))
    .withColumn("Population 2020", df("Population 2020").cast(IntegerType))
    .withColumn("Density per Mile", df("Density (/mile2)").cast(IntegerType))
    .withColumn("Relative Annual Change", df("Annual Change").cast(DecimalType(5, 4)))
    .drop("Annual Change")
    .withColumn("Area in Square Miles", df("Area (mile2)").cast(DecimalType(8, 2)))
    .drop("Area (mile2)")

  df = df.filter("Rank < 11")
  
  df.show()
  df.dtypes.map(println)
    
  df.groupBy().sum("Population 2024").show()

