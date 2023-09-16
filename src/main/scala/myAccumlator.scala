import org.apache.spark.SparkContext

object myAccumlator extends App {

  val sc = new SparkContext("local[*]","myaccumulartor")
  val myaccumulator = sc.longAccumulator("blank line accumulator")
  val input = sc.textFile("/home/rithwick/Downloads/acumularfile.txt")
  input.foreach(x=> if(x=="")myaccumulator.add(1))
  println(myaccumulator.value)

}
