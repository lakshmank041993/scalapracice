import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


case class Orders(order_id:Int, customer_id:Int, order_status:String)

object week12LoadUnstructuredFileToRddtoDF extends App{

  def parser(line:String)= {
  line match {
    case regex(order_id,date,time,customer_id,order_status) =>
      Orders(order_id.toInt,customer_id.toInt,order_status)
  }
  }
  // spark conf
  val myConf = new SparkConf()
  myConf.set("spark.app.name","load unstructured file to rdd to Ds")
  myConf.set("spark.master","local[*]")

  //spark session
  val spark = SparkSession.builder().config(myConf).getOrCreate()

  // load file using park context
  val fileLoad = spark.sparkContext.textFile("/home/rithwick/Downloads/testorders.csv")
  val regex = """^(\S+) (\S+) (\S+)\t\t(\S+)\,(\S+)""".r
  val structruredRdd =fileLoad.map(parser)

  import spark.implicits._

  // convert to Ds and cached the Ds
  val orderDf = structruredRdd.toDS().cache()

  orderDf.printSchema()
  orderDf.select("order_id").show()
  orderDf.groupBy("order_status").count().show()

  spark.stop
}
