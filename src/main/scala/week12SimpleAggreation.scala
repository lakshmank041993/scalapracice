import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.log4j.{Level, Logger}
object week12SimpleAggreation extends App {

  // spark conf
  // Set the log level to ERROR for Spark
  val nConf = new SparkConf()
  nConf.set("spark.app.name","simple aggregation")
  nConf.set("spark.master","local[2]")

  // sparkSession

  val spark = SparkSession.builder().config(nConf).getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  // load the csv using standard data frame api

  val ordersDf = spark.read.format("csv").option("inferSchema","true").option("header","true").option("path","/home/rithwick/Downloads/order_data-201025-223502.csv").load()
 // ordersDf.show()

  // calculation using column object expression
  import spark.implicits._
  import functions._

  ordersDf.select(count("*").as("total_count"),sum("Quantity").as("total_quantity"), avg("UnitPrice").as("avg_unit_price"), countDistinct("InvoiceNo").as("distinct_invoice")).show

  // using string expression
  ordersDf.selectExpr("count(*) as row_count","sum(Quantity) as sum_quantity","avg(UnitPrice) as avg_price","count(distinct(InvoiceNo)) as distinct_invoice").show()
  // stop spark session


  // using spark sql
  ordersDf.createOrReplaceTempView("sales")
  spark.sql("select count(*) , sum(Quantity), avg(UnitPrice) , count(distinct(InvoiceNo)) from sales").show() // this will return a data frame


  spark.stop()

}
