import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object testfile extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "wordcount")
  val myList = List(
    "WARN:  Tuesday 4 September 0405",
    "ERROR: Tuesday 4 September 0408",
    "ERROR: Tuesday 4 September 0408",
    "ERROR: Tuesday 4 September 0408",
    "ERROR: Tuesday 4 September 0408",
    "ERROR: Tuesday 4 September 0408"
  )
  val originallogsRdd = sc.parallelize(myList).map(x=> (x.split(":")(0),1))

  val ResultantRdd = originallogsRdd.reduceByKey((x, y) => x + y)
  ResultantRdd.collect().foreach(println)
}