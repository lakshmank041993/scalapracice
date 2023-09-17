import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object week12SparkSql extends App {

  // spark conf
  val myConf =new SparkConf()
  myConf.set("spark.app.name","spark sql")
  myConf.set("spark.master","local[*]")
  // spark session
  val spark = SparkSession.builder.config(myConf).getOrCreate()
  val csvLoad = spark.read.format("csv").option("header", value = true).option("inferSchema",value = true)
    .option("path","/home/rithwick/Downloads/orders-201025-223502.csv").load
  // create spark sql api
  csvLoad.createOrReplaceTempView("orders")
  // query spark sql and print the df
  val resultDf =spark.sql("select order_status , count(*) as total_status from orders group by order_status order by total_status desc ")
  val numberOrdersCustomerOrder = spark.sql("select order_customer_id , count(*) as total_orders from orders where order_status = 'CLOSED' "+
    "group by order_customer_id order by total_orders desc")
  resultDf.show
  numberOrdersCustomerOrder.show(100)
  spark.stop()

}
