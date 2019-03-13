package pr

import scala.collection.JavaConversions._

import au.com.bytecode.opencsv.CSVWriter
import java.io.{BufferedWriter, FileWriter}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager

object Pagerank {

  def pow2(v: Int)= v*v

  def makeGraph( k: String, input: String) {
    val out = new BufferedWriter(new FileWriter(input))
    val writer = new CSVWriter(out)

    for(i <- 1 to pow2(k.toInt)) {
      

    }

    val listOfRecords = List(col1,col2,col3)

    writer.writeAll(listOfRecords)
    out.close()
  }
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\nfc.FollowerCountMain <input dir> <output dir> <param-k>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Pagerank")
    val sc = new SparkContext(conf)

    val col1= Array("1","2","3")
    val col2= Array("4","5","6")
    val col3= Array("7","8","9")

    makeGraph(args{2}, args{0})
    
    //Read input file
    val textFile = sc.textFile(args(0))
    
    //fetch the user_id_b, from a given input as user_id_a,user_id_b
    val counts = textFile.flatMap(line => line.split(",").tail)
    
    //mapper function to generate user_id,no_of_followers 
            .map(user_Id => (user_Id,1))

    //reducer function to generate final output as user_id, total_no_of_followers
	          .reduceByKey(_+_)

    counts.saveAsTextFile(args(1))

    println(counts.toDebugString)
  }
}
