import org.apache.spark.SparkContext

object movieRatingWithCountByVal extends App{

  // creating spark context
  val sc =  new SparkContext("local[*]","movie rating")
  // import data from file to rdd
  val input = sc.textFile("/home/rithwick/Downloads/moviedata-201008-180523.data")
  // get the split val
  val rating = input.map(x => x.split("\t")(2))
  // use count by value
  val finalRating = rating.countByValue
  // print value
  finalRating.foreach(println)
}
