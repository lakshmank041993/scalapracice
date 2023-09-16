import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object dataFrameExample extends App {
  // set spark config
  val sConf = new SparkConf()
  sConf.set("spark.app.name","dataFrame Example")
  sConf.set("spark.master","local[*]")
  // create spark session
  val sSession = SparkSession.builder().config(sConf).getOrCreate()

  // create dataframe
  // below is a data frame reader
  val orderDF :Dataset[Row] =sSession.read.option("header",true).option("inferSchema",true).csv("/home/rithwick/Downloads/orders-201019-002101.csv")

  orderDF.filter("order_id < 10").show // give op since column name is matching
  //orderDF.filter("order_ids < 10").show //gives run time error since headers is not matching

  sSession.stop()

}
