import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object week12simpleJoinDf extends App {

  // create spark conf
  val conf = new SparkConf()
  conf.set("spark.app.name","simple join in df")
  conf.set("spark.master","local[2]")

  // spark session
  val spark = SparkSession.builder().config(conf).getOrCreate()
  // setting logger level using spark conf
  spark.sparkContext.setLogLevel("ERROR")

  // read data
  val ordersDf =spark.read.format("json").option("path","/home/rithwick/Downloads/orders_week12_join").load()
  ordersDf.show()
  val customersDf =spark.read.format("json").option("path","/home/rithwick/Downloads/customers_week12_join").load()
  customersDf.show()

  // join the both data set

  spark.sql("SET spark.sql.autoBroadcastJoinThreshold = -1")

  val joinedDf = ordersDf.join(customersDf,ordersDf.col("order_customer_id")=== customersDf.col("customer_id"),"inner")

  //val rightJoinedDf = ordersDf.join(customersDf,ordersDf.col("order_customer_id")=== customersDf.col("customer_id"),"right").sort("order_id")
  joinedDf.show()

  //rightJoinedDf.show()

  scala.io.StdIn.readLine()
  spark.stop()
}
