import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
object test extends App {

  val myConf = new SparkConf()
  myConf.set("spark.app.name", "write as DB")
  myConf.set("spark.master", "local[*]")

   val random = new scala.util.Random
   val start = 1
   val end = 40
  val spark = SparkSession.builder().config(myConf).getOrCreate()
  val sc = spark.sparkContext
   val rdd1 =sc.textFile("/home/rithwick/Downloads/bigLog.txt")
   rdd1.collect.foreach(println)




}