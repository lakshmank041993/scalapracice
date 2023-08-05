import org.apache.spark.SparkContext

import scala.util.control.Breaks._

object top10Words extends App{

  val sc = new SparkContext("local[*]","wordCpunt")
  val fileLoad = sc.textFile("/home/rithwick/Downloads/import.cdn.thinkific.com_349536_search_data-201008-180523.txt")
  val wordSplit = fileLoad.flatMap(x=>x.split(" ")) // splits by space and flatens the list
  val wordlower = wordSplit.map(x => x.toLowerCase()) // transforms to lowercase
  val wordTuple = wordlower.map(x => (x,1)) // assign value to 1
  val wordCount =wordTuple.reduceByKey((x,y) => x+y) // reduce function to add the key value
  val reversedtuple = wordCount.map{case(x,y) => (y,x)} // revere the order of tuple
  val sortTuple = reversedtuple.sortByKey(false)// sort by descending order
  val finalreverse = sortTuple.map{case(x,y) => (y,x)} // revere the order of tuple
  val results = finalreverse.collect() // action function
  var top10 = 0  // initialize te count to zero
  breakable {
    for (x <- results) {   // loop in the result , print top 10 values and break the for loop
      val (word, count) = x
      println(s"$word,$count")
      top10 += 1
      if (top10 >10){
        break
      }

    }
  }
}
