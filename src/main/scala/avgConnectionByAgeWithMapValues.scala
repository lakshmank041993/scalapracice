import org.apache.spark.SparkContext

object avgConnectionByAgeWithMapValues extends App {

  def parceval(lines: String) = {
    val fields = lines.split("::")
    val age = fields(2).toInt
    val connections = fields(3).toInt
    (age, connections)
  }

  // create an spark Context
  val sc = new SparkContext("local[*]", "avg connections by age")
  //import data to rdd
  val input = sc.textFile("/home/rithwick/Downloads/friendsdata-201008-180523.csv")
  //split using map
  val ageConnection = input.map(parceval)
  // transform (key,value) => (key,(value,1))
  //eg (30,100) => (30,(100,1))
  // val mapval = ageConnection.map(x => (x._1, (x._2, 1)))
  val mapval = ageConnection.mapValues(x => (x,1))
  // (30,(100,1))
  //(30,(200,1))
  // (30,(300,1)) => (30,(600,3))
  val aggreVal = mapval.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
  // => (30,(600,3)) => (30, 600/3) => (30, 200)
  //val finalVal = aggreVal.map(x => (x._1, x._2._1 / x._2._2))
  val finalVal = aggreVal.mapValues(x => (x._1/x._2))
  // sort by column 2
  val sortedval = finalVal.sortBy(x => x._2)

  sortedval.collect.foreach(println)

}
