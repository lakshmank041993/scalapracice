import org.apache.spark.SparkContext

object addStatusBasedOnAge extends App {

  val sc = new SparkContext("local[*]","add stats based on age")
  val input = sc.textFile("/home/rithwick/Downloads/201125-161348.dataset1")
  val splitVal =input.map(x =>x.split(",")).map(x => (x(0),x(1),x(2),if (x(1).toInt > 18) "Y" else "N"))
  val result = splitVal.collect()
  for (i <- result){
    println(i)
  }

}
