import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}


object week12ColumnStringNotation extends App {

  // spark conf
  val myConf = new SparkConf()
  myConf.set("spark.app.name", "write as DB")
  myConf.set("spark.master", "local[*]")

  // spark session
  val spark = SparkSession.builder.config(myConf).enableHiveSupport().getOrCreate()

  // load file

  val csvLoad = spark.read.format("csv").option("header", value = true).option("inferSchema", value = true)
    .option("path", "/home/rithwick/Downloads/orders-201025-223502.csv").load

  csvLoad.select("order_id","order_status").show




}
