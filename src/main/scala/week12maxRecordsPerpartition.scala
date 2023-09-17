import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import week12PartitionBySpark.{csvload, spark}

object week12maxRecordsPerpartition  extends App{

  // create conf
  val myConf =new SparkConf()
  myConf.set("spark.app.name","max records per file")
  myConf.set("spark.master","local[*]")

  // spark session
  val spark = SparkSession.builder.config(myConf).getOrCreate()

  // file load

  val csvload = spark.read.format("csv").option("header", true).option("inferSchema", true).option("path", "/home/rithwick/Downloads/orders-201025-223502.csv").load

  // file sink with maxrecords per file

  csvload.write.format("csv").partitionBy("order_status").option("maxRecordsPerFile",500).mode(SaveMode.Overwrite).option("path", "/home/rithwick/Downloads/week12_op_json_maxRecordsPerfile_folder").save

  spark.stop()


}
