import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object min_temp extends App{

  def minimum(x:Float, y:Float) = {
    if (x > y) {y}
    else {x}
  }
  
 Logger.getLogger("org").setLevel(Level.ERROR)
 
 val sc = new SparkContext("local[*]","min_temp")
 val input = sc.textFile("/Users/gupta/Downloads/datasets/tempdata.csv")		// replace the path accordingly
 
 val mappedInput = input.map( x => (x.split(",")(0),
                                    x.split(",")(3).toFloat) )      // get the station_id and temp_recorded
                                    
 val minTemp = mappedInput.reduceByKey((x,y) => minimum(x,y))       
                                    
 val result = minTemp.collect()
 
  //displaying the result
 for (res <- result) {
   val station = res._1
   val mintemp = res._2
   println(s"$station minimum temperature: $mintemp F")
 }

}
