import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

object week12ColumnExpression extends  App {

  // spark conf
  val myConf = new SparkConf()
  myConf.set("spark.app.name", "write as DB")
  myConf.set("spark.master", "local[*]")

  // spark session
  val spark = SparkSession.builder.config(myConf).enableHiveSupport().getOrCreate()

  // load file

  val csvLoad = spark.read.format("csv").option("header", value = true).option("inferSchema", value = true)
    .option("path", "/home/rithwick/Downloads/orders-201025-223502.csv").load

  //csvLoad.select("order_id", "order_status").show

  // cannot mix col string , col object , col expr
  csvLoad.select(col("order_id"),expr("concat(order_status,'_STATUS') as status")).show(false)
  csvLoad.selectExpr("order_id","concat(order_status,'_STATUS') as status","order_date").show(false)


}
