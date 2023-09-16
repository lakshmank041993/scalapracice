import org.apache.spark.SparkContext

object top10CustomersCacheInScala extends App {
  val sc = new SparkContext("local[*]", "shophoholic")
  val input = sc.textFile("/home/rithwick/Downloads/customerorders-201008-180523.csv")
  val custmerOrderPrice = input.map(x => (x.split(",")(0), x.split(",")(2).toFloat))
  val customerOrdersPricetoInt = custmerOrderPrice.reduceByKey((x, y) => x + y)
  val sortedTotal = customerOrdersPricetoInt.filter(x => x._2 > 5000)

  val doubledAmount = sortedTotal.map(x => (x._1,x._2*2)).cache()
  val result =  doubledAmount.collect()
  result.foreach(println)
  println(doubledAmount.count)
  scala.io.StdIn.readLine()
}
