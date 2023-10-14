import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.sum

object week12WindowAggregaration extends App {

    // spark conf

  val mConf = new SparkConf()
  mConf.set("spark.app.new","example")
  mConf.set("spark.master","local[2]")

  // spark session
  val spark = SparkSession.builder().config(mConf).getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  // load using standard data frame api

  val windowDf = spark.read.format("csv").option("header","false").option("inferSchema","true").option("path","/home/rithwick/Downloads/windowdata-201025-223502.csv").load()

  val windowDfHeader = windowDf.toDF("country","weeknum","numinvoice","totalquantity","invoicevalue")
  windowDfHeader.show()
  val mywindow =Window.partitionBy("country").orderBy("weeknum").rowsBetween(Window.unboundedPreceding,Window.currentRow)

  val myDf =windowDfHeader.withColumn("runningtotal",sum("invoicevalue").over(mywindow))
  myDf.show(40)

  spark.stop()

}
