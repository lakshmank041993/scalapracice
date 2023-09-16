import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

object top10CustomePersistInScala extends App {
  val sc = new SparkContext("local[*]", "shophoholic")
  val input = sc.textFile("/home/rithwick/Downloads/customerorders-201008-180523.csv")
  val custmerOrderPrice = input.map(x => (x.split(",")(0), x.split(",")(2).toFloat))
  val customerOrdersPricetoInt = custmerOrderPrice.reduceByKey((x, y) => x + y)
  val sortedTotal = customerOrdersPricetoInt.filter(x => x._2 > 5000)

  //val doubledAmount = sortedTotal.map(x => (x._1,x._2*2)).persist()
  //val doubledAmount = sortedTotal.map(x => (x._1,x._2*2)).persist(StorageLevel.MEMORY_ONLY)
  // if we use cache or memoery only and we dont have enough it will skip the step but not
  val doubledAmount = sortedTotal.map(x => (x._1,x._2*2)).persist(StorageLevel.MEMORY_AND_DISK)

  val result = doubledAmount.collect()
  result.foreach(println)
  println(doubledAmount.count)
  scala.io.StdIn.readLine()
}
