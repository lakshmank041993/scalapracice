
import org.apache.spark.SparkContext

object assignmentWeek10 extends App {

  val sc = new SparkContext("local[*]", "spark assignment ")

  val chapeterBaseRdd = sc.textFile("/home/rithwick/Downloads/chapters-201108-004545.csv").map(x => {
    val chapterDatafiles = x.split(",")
    (chapterDatafiles(0).toInt, chapterDatafiles(1).toInt)
  })

  val viewsBaseRdd = sc.textFile("/home/rithwick/Downloads/views*.csv").map(x => {
    val viewDatafiles = x.split(",")
    (viewDatafiles(0), viewDatafiles(1).toInt)
  })

  val titlesBaseRdd = sc.textFile("/home/rithwick/Downloads/titles-201108-004545.csv").map(x => {
    val chapterDatafiles = x.split(",")
    (chapterDatafiles(0).toInt, chapterDatafiles(1))
  })

  val chapterCountRdd = chapeterBaseRdd.map(x => (x._2, 1)).reduceByKey((x, y) => x + y)

  val viewsDistinceRdd = viewsBaseRdd.distinct()

  val flippedviewRdd = viewsDistinceRdd.map(x => (x._2, x._1))

  val joinedRdd = flippedviewRdd.join(chapeterBaseRdd)

  val pairRdd = joinedRdd.map(x => ((x._2._1, x._2._2), 1))

  // finding out the number of count of numbers a user has watched per course
  val userPerCourseVieRdd = pairRdd.reduceByKey((x,y)=>(x+y))

  val courseViewCountRdd = userPerCourseVieRdd.map(x =>(x._1._2,x._2))
  // join chapterRdd cource view rdd
  val newJoinedRdd = courseViewCountRdd.join(chapterCountRdd)
  // calculater percentage

  val calculatePercentageRdd = newJoinedRdd.mapValues(x=>x._1.toDouble/x._2)

  val formattedpercentageRDD =  calculatePercentageRdd.mapValues(x => (f"$x%01.5f".toDouble))
  val scoresRDD =  formattedpercentageRDD.mapValues (x =>
  {if(x >= 0.9  )  10
  else if(x >=  0.5 &&  x < 0.9  )  4
  else if(x >=  0.25 &&  x < 0.5) 2
  else 0} )

  val totalScorePerCourseRDD =   scoresRDD.reduceByKey((V1,V2) => V1 + V2)
  val title_score_joinedRDD =  totalScorePerCourseRDD.join(titlesBaseRdd).map  ( x => (x._2._1, x._2._2))
  val popularCoursesRDD = title_score_joinedRDD.sortByKey(false)
  popularCoursesRDD.collect.foreach(println)


}