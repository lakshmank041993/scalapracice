import org.apache.spark.SparkContext

object groupByKeyLogLevelWordCount extends App{
    val sc = new SparkContext("local[*]","word count logger level")
    // load the logger file to rdd
    val input = sc.textFile("/home/rithwick/Downloads/bigLog.txt")
    // split the lines with :
    val rdd1 = input.map(x => {
      val lines = x.split(":")
      (lines(0),lines(1))
    })
    rdd1.groupByKey().collect().foreach(x => println(x._1,x._2.size))
    scala.io.StdIn.readLine()
}
