import org.apache.spark.SparkContext

object lowestTempForStation extends App {

  val sc = new SparkContext("local[*]","lowest temp recorded")
  val input = sc.textFile("/home/rithwick/Downloads/tempdata-201125-161348.csv")
  val splitVal = input.map(x => x.split(",")).map(x => (x(0),(1,x(3).toInt)))
  val reduceVal = splitVal.reduceByKey((x, y) => if(x._2<y._2) x else y).map(x => (x._1,x._2._2))
  val result = reduceVal.collect()
  result.foreach(println)
}




