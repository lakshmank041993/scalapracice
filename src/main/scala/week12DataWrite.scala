import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object week12DataWrite extends App {

  val myconf = new SparkConf()
  myconf.set("spark.app.name","data load")
  myconf.set("spark.master","local[*]")

  val spark = SparkSession.builder.config(myconf).getOrCreate()

  // data load from csv
  val csvload = spark.read.format("csv").option("header",true).option("inferSchema",true).option("path","/home/rithwick/Downloads/orders-201025-223502.csv").load

  // write to folder
  csvload.write.format("json").mode(SaveMode.Overwrite).option("path","/home/rithwick/Downloads/week12_op_parquet_folder").save

  // writes by default into a sink in parquet
  csvload.write.mode(SaveMode.Overwrite).option("path","/home/rithwick/Downloads/week12_op_parquet_folder").save

  // Stop spark
  spark.stop()
}
