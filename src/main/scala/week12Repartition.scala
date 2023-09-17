import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object week12Repartition extends App {

  // load conf
  val myConf = new SparkConf()
  myConf.set("spark.app.name","repartition")
  myConf.set("spark.master","local[*]")

  // spark session

  val spark = SparkSession.builder.config(myConf).getOrCreate()
  // load file
  val csvload = spark.read.format("csv").option("header",true).option("inferSchema",true).option("path","/home/rithwick/Downloads/orders-201025-223502.csv").load

  //check partions

  println("number partitions",csvload.rdd.getNumPartitions)
  println("*---------------***-------------------****--------------***------------**")

  val repartionLoad = csvload.repartition(3)

  println(repartionLoad.rdd.getNumPartitions)
  println("*-------************------------------------************----------------*")

  csvload.write.format("csv").mode(SaveMode.Overwrite).option("path","/home/rithwick/Downloads/week12_op_json_repartition_folder").save

  spark.stop


}
