package pr

import scala.collection.JavaConversions._
import au.com.bytecode.opencsv.CSVWriter
import java.io.{BufferedWriter, FileWriter}
import java.util

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD

object Pagerank {

  //returns square of the integer input
  def pow2(v: Int)= v*v

  //Make the row of our graph based on the input parameter k
  def makeRow(x: Int, k: Int): Array[String] = {
    var i = x

    val sb = StringBuilder.newBuilder

    while(i <= k) {
      sb.append(i.toString)
      if(i != k){
        sb.append(",")
      }
      i+=1
    }

    val str : Array[String] = sb.toString().split(",")

    str
  }

  //Make the input based on the input parameter k and store it in input file input.csv
  def makeInput(k: String, input: String) {
    val out = new BufferedWriter(new FileWriter(input+"/input.csv"))
    val writer = new CSVWriter(out)
    val x = k.toInt

    val listOfRecords = new java.util.ArrayList[Array[String]]()

    for(i <- 1 to pow2(x) by x) {
      listOfRecords.add(makeRow(i, i+x-1))
    }

    writer.writeAll(listOfRecords)
    out.close()
  }

  //Make the graph based on the input
  def makeGraph(input: RDD[String]): Unit = {
    
  }
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\nfc.FollowerCountMain <input dir> <output dir> <param-k>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Pagerank")
    val sc = new SparkContext(conf)

    makeInput(args{2}, args{0})

    //Read input file
    val textFile = sc.textFile(args(0))

    makeGraph(textFile)

//
//    //fetch the user_id_b, from a given input as user_id_a,user_id_b
//    val counts = textFile.flatMap(line => line.split(",").tail)
//
//    //mapper function to generate user_id,no_of_followers
//            .map(user_Id => (user_Id,1))
//
//    //reducer function to generate final output as user_id, total_no_of_followers
//	          .reduceByKey(_+_)
//
//    counts.saveAsTextFile(args(1))
//
//    println(counts.toDebugString)
  }
}
