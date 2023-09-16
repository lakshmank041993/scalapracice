import org.apache.spark.SparkContext

object movieRatingCount extends App{
  // create spark context
  val sc = new SparkContext("local[*]","movie rating")
  // import the file to rdd
  val input = sc.textFile("/home/rithwick/Downloads/moviedata-201008-180523.data")
  // fetch only the rating val
  val rating = input.map(x => x.split("\t")(2))
  // assign value to rating
  val ratingCount = rating.map(x => (x,1))
  // aggrigate value
  val totalRating = ratingCount.reduceByKey((x,y)=>x+y)
  // final action
  totalRating.collect.foreach(println)
}