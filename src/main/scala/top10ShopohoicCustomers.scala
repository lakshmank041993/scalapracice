import org.apache.spark.SparkContext

object top10ShopohoicCustomers extends App{
  val sc = new SparkContext("local[*]","shophoholic")
  val input = sc.textFile("/home/rithwick/Downloads/customerorders-201008-180523.csv")
  val custmerOrderPrice = input.map(x => (x.split(",")(0),x.split(",")(2).toFloat))
  val customerOrdersPricetoInt = custmerOrderPrice.reduceByKey((x,y) => x+y)
  val sortedTotal = customerOrdersPricetoInt.sortBy(x => x._2)
  //val result =  sortedTotal.collect()
  //result.foreach(println)
  sortedTotal.saveAsTextFile("/home/rithwick/Downloads/top10ShopohoicCustomers_op")
}
