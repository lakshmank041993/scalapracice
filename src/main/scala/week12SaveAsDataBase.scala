import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object week12SaveAsDataBase extends App{

  // spark conf
  val myConf = new SparkConf()
  myConf.set("spark.app.name","write as DB")
  myConf.set("spark.master","local[*]")

  // spark session
  val spark = SparkSession.builder.config(myConf).config("spark.sql.catalogImplementation", "in-memory").getOrCreate()

  // load file

  val csvLoad = spark.read.format("csv").option("header", value = true).option("inferSchema", value = true)
    .option("path", "/home/rithwick/Downloads/orders-201025-223502.csv").load


  // create database

  spark.sql("CREATE DATABASE IF NOT EXISTS retail")

  // write as data base
  csvLoad.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .saveAsTable("order5")

  //list all the tables in database
  spark.catalog.listTables("retail").show

  spark.stop()


}
