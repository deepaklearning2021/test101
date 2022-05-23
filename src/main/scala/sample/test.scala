package sample
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger
object test {

  def main(args:Array[String]):Unit={

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf=new SparkConf().setAppName("wordcount1").setMaster("local[*]")
      .set("spark.hadoop.validateOutputSpecs","false") //to overwrite the directory everytime if you are writing the files
    val sc=new SparkContext(conf)



    //    val spark=SparkSession.builder.getOrCreate()
    //    import spark.implicits._

    val input_rdd = sc.textFile("file:///C:/SparkScala/input_data/word_count.txt")
    val flatten_data = input_rdd.flatMap(x=>x.split(" "))
    val assign_one=flatten_data.map(word=>(word,1))
    val result = assign_one.reduceByKey((x,y) => x+y)
    //    result.foreach(println)
    println("*****************")
    val sorted_result = result.sortBy(x=>x._2)
    sorted_result.foreach(println)

    sorted_result.coalesce(1).saveAsTextFile("file:///C:/SparkScala/input_data/wordcount_output")

    sc.stop()
    println("added to master")
  }

}
