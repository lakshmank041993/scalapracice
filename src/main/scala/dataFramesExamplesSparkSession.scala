import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object dataFramesExamplesSparkSession extends App {

  val conf = new SparkConf()
  conf.set("spark.app.name","myapplication")
  conf.set("spark.master","local[2]")
  val spark = SparkSession.builder().appName("myapplication1").master("local[2]").getOrCreate()
  spark.stop()

}
