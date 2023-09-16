import org.apache.spark.SparkContext

object TrendyTechSearchData extends App{

  val sc = new SparkContext("local[*]","trendy tech search words")
  val inputVal = sc.textFile("/home/rithwick/Downloads/bigdatacampaigndata-201014-183159.csv")
  val normalisedVal = inputVal.map(x => x.split(",")).map(x => (x(10).toFloat,x(0)))
  /*big data content, 24.06
  #learning big data, 34.20

  #transformation to
  #24.06, big data content
  #34.20, learing big data

   */
  val flatenKey = normalisedVal.flatMapValues(x => x.split(" "))
  /*
  #(24.06, big)
  #(24.06, data)
  #(24.06, content)
  #(34.20, learning)
  #(34.20, big)
  #(34.20, data)
  #flatten out the column 2
   */
  val reveredTuple = flatenKey.map(x => (x._2,x._1))
  val redusedVal = reveredTuple.reduceByKey((x,y)=> x+y)
  val sortedVal = redusedVal.sortBy(x => x._2, ascending = false)
  //val finalOutput = sortedVal.collect()
  val finalOutput = sortedVal.take(20)
  finalOutput.foreach(println)

}
