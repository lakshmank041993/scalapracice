import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

case class windowData(Country: String, Weeknum: Int, NumInvoices: Int, TotalQuantity: Int,
                      InvoiceValue: String)
object week11AssRddToDfStoreToJson extends App{

  // spark conf
  val myconf =new SparkConf()
  myconf.set("spark.app.name","week 11 application")
  myconf.set("spark.master","local[*]")

  val spark =SparkSession.builder().config(myconf).getOrCreate()
  import spark.implicits._

  val windowload = spark.sparkContext.textFile("/home/rithwick/Downloads/windowdata-201021-002706.csv")

  val windowDF =windowload.map(x => x.split(",")).map(x => windowData(x(0), x(1).trim.toInt, x(2).trim.toInt,x(3).trim.toInt, x(4))).toDF.repartition(8)
  windowDF.write.format("json").mode(SaveMode.Overwrite).option("path","/home/rithwick/Downloads/json_week11_assignment_op").save()

  windowDF.show()
  spark.stop()

}
