import org.apache.spark.SparkContext

object reduceAction extends App{

  val sc =new SparkContext("local[*]", "reduce key")
  val range = 1 to 100
  val rdd1 =sc.parallelize(range)
  println(rdd1.reduce((x,y) => x+y))
}
