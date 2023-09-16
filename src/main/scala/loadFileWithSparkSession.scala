import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object loadFileWithSparkSession extends App {
  Logger.getLogger("org")setLevel Level.ERROR
  val conf = new SparkConf()
  conf.set("spark.app.name", "myapplication")
  conf.set("spark.master", "local[2]")
  val spark = SparkSession.builder().appName("myapplication1").master("local[2]").getOrCreate()
  val input = spark.read.option("Header",true).option("inferSchema",true).csv("/home/rithwick/Downloads/orders-201019-002101.csv")
  val orderDf = input.repartition(4).
    where("order_customer_id > 10000").
    select("order_id","order_customer_id").
    groupBy("order_customer_id").count()
  orderDf.foreach(x=>{
    println(x)
  })
  orderDf.show()
  Logger.getLogger(getClass.getName).info("my application is completed successfully")
  //scala.io.StdIn.readLine()
  spark.stop()


}
