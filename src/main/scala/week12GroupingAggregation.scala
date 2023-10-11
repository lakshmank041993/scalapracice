import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object week12GroupingAggregation extends App{

  // spark conf
  val mConf= new SparkConf()
  mConf.set("spark.app.name","groping aggregation")
  mConf.set("spark.master","local[2]")

  // spark session
  val spark = SparkSession.builder().config(mConf).getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  // spark standard dataframe api
  val invoiceDf = spark.read.format("csv").option("inferSchema","true").option("header","true").option("path","/home/rithwick/Downloads/order_data-201025-223502.csv").load()
  // invoiceDf.show()
  // group by function
  //+---------+---------+--------------------+--------+---------------+---------+----------+--------------+
  //|InvoiceNo|StockCode|         Description|Quantity|    InvoiceDate|UnitPrice|CustomerID|       Country|
  // using column expression
  invoiceDf.groupBy("Country","InvoiceNo").agg(sum("Quantity").as("totalQuantity"), sum(expr("Quantity * UnitPrice")).as("invoice-val")).show()

  // using string expression
  invoiceDf.groupBy("Country","InvoiceNo").agg(expr("sum(Quantity) as totalQuantity"), expr("sum(Quantity * UnitPrice) as invoiceval")).show()

  // spark sql
  invoiceDf.createOrReplaceTempView("sales")
  spark.sql("select Country , InvoiceNo, sum(Quantity), sum(Quantity * UnitPrice) from sales group by Country , InvoiceNo").show()

  spark.stop()


}
/*+--------------+---------+-------------+---------------------------+
|       Country|InvoiceNo|sum(Quantity)|sum((Quantity * UnitPrice))|
+--------------+---------+-------------+---------------------------+
|United Kingdom|   536446|          329|                     440.89|
|United Kingdom|   536508|          216|                     155.52|
|United Kingdom|   537811|           74|                     268.86|
|United Kingdom|   538895|          370|                     247.38|
|United Kingdom|   540453|          341|         302.44999999999993|
|United Kingdom|   541291|          217|         305.81000000000006|
|United Kingdom|   542551|           -1|                        0.0|
|United Kingdom|   542576|           -1|                        0.0|
|United Kingdom|   542628|            9|                     132.35|
|United Kingdom|   542886|          199|          320.5099999999998|
|United Kingdom|   542907|           75|                     313.85|
|United Kingdom|   543131|          134|                      164.1|
|United Kingdom|   543189|          102|                     153.94|
|United Kingdom|   543265|           -4|                        0.0|
|        Cyprus|   544574|          173|                     320.69|
|United Kingdom|   545077|           24|                      10.08|
|United Kingdom|   545300|          116|                     323.16|
|United Kingdom|   545347|           72|          76.32000000000001|
|United Kingdom|   545418|           10|                       85.0|
|United Kingdom|   545897|          577|         1762.2200000000018|
+--------------+---------+-------------+---------------------------+
only showing top 20 rows */
