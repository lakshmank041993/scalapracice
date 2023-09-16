import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object WordCountFromLoacalVariable extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  // create a spark context
  val sc = new SparkContext("local[*]", "word count ")
  // value in a local variable
  val mylist = List("WARN: Hello timestamp1",
    "ERROR: hello timestamp2",
    "ERROR: hello timestamp1",
    "WARN: hello timestamp4",
    "ERROR: hello timestamp5",
    "ERROR: hello timestamp5",
    "INFO: hello timestamp6")

  // load the local variable content to rdd
  val input = sc.parallelize(mylist)
  // split the value of the content between ERROR and rest of the text => which recembles (ERROR,1)
  val logLevel= input.map(x =>{
   val column = x.split(":")
   val logLevel = column(0)
    (logLevel,1)
  })
  // final result after aggregation
  val finalVal = logLevel.reduceByKey((x,y)=>x+y)
  finalVal.foreach(println)
}