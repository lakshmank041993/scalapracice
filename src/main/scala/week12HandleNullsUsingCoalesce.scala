import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object week12HandleNullsUsingCoalesce extends App{

  // create spark conf
  val conf = new SparkConf()
  conf.set("spark.app.name", "simple join in df")
  conf.set("spark.master", "local[2]")

  // spark session
  val spark = SparkSession.builder().config(conf).getOrCreate()
  // setting logger level using spark conf
  spark.sparkContext.setLogLevel("ERROR")

  // read data
  val ordersDf = spark.read.format("csv").option("header","true").option("inferSchema","true").option("path", "/home/rithwick/Downloads/orders-201025-223502.csv").load()
  ordersDf.show()
  //val ordersNew =ordersDf.withColumnRenamed("order_customer_id","cust_id")
  val customersDf = spark.read.format("csv").option("header","true").option("inferSchema","true").option("path", "/home/rithwick/Downloads/customers-201025-223502.csv").load()
  customersDf.show()

  // join the both data set

  val joinedDf = ordersDf.join(customersDf, ordersDf.col("order_customer_id") === customersDf.col("customer_id"), "outer").drop(ordersDf.col("order_customer_id"))

  //val rightJoinedDf = ordersNew.join(customersDf, ordersNew.col("cust_id") === customersDf.col("customer_id"), "inner").sort("order_id")
  joinedDf.select("customer_id","order_id").sort("order_id").withColumn("order_id",expr("coalesce(order_id,'new customer')")).show()

 // rightJoinedDf.select("cust_id","customer_email","customer_state").show()
  spark.stop()

}
