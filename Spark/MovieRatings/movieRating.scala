import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object movieData extends App{
  /*user_id, movie_id, rating, unix_timestamp*/
  /*How many times movies were rated 5, 4, 3, 2, 1 star*/
  
  Logger.getLogger("org").setLevel(Level.ERROR)     // Setting the logging level to ERROR
  
  val sc = new SparkContext("local[*]", "moviedata")    
  
  val input = sc.textFile("/Users/gupta/Downloads/datasets/moviedata.data")  
  
  val mappedInput = input.map( x => ( x.split("\t")(2).toInt ))    // taking only third column as only it is required.
                
  val sortMapped = mappedInput.map(x => (x,1))

  val reduced = sortMapped.reduceByKey((x,y) => x+y)

  val result = reduced.sortBy(x => x._2).collect()
  
  result.foreach(println)

}
