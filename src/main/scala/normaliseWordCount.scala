import org.apache.log4j.{Logger,Level}
import org.apache.spark.SparkContext

object normaliseWordCount extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "wordCount")
  val input = sc.textFile("/home/rithwick/Downloads/import.cdn.thinkific.com_349536_search_data-201008-180523.txt")
  val words = input.flatMap(x => x.split(" "))
  val wordslower = words.map(x => x.toLowerCase())
  val wordMap = wordslower.map(x => (x, 1))
  val wordCount = wordMap.reduceByKey((x, y) => x + y)
  wordCount.collect.foreach(println)
}
