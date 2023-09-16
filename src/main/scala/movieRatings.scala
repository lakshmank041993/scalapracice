import org.apache.spark.SparkContext

object movieRatings extends App{
  //create spark context
  val sc = new SparkContext("local[*]","movie ratings")

  val movieNameRDD = sc.textFile("/home/rithwick/Downloads/movies-201019-002101.dat").map(x=> {
    val movieFileds = x.split("::")
    (movieFileds(0).toInt,movieFileds(1))
  })

  // 1::1193::5::978300760
  val movieRatingRDD = sc.textFile("/home/rithwick/Downloads/ratings-201019-002101.dat").map(x => {
    val fields = x.split("::")
    (fields(1),fields(2))
  }) // == (1193,5)
  val newMovieRatingRdd = movieRatingRDD.map(x=> (x._1,(x._2.toFloat,1.0)))
  val aggreMovierating = newMovieRatingRdd.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2) )
  val filteredMovierating = aggreMovierating.filter(x=> x._2._2 > 1000)
  val movieRatingFiltered = filteredMovierating.mapValues(x => x._1/x._2).filter(x=> x._2 > 4.0).map(x=>(x._1.toInt,x._2))
  val joinedData = movieRatingFiltered.join(movieNameRDD)
  val finalmoviesRDD = joinedData.map(x=>x._2._2)
  finalmoviesRDD.collect.foreach(println)
  //scala.io.StdIn.readLine()
}
