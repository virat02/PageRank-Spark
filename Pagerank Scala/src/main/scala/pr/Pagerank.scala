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
  def makeRow(x: Int, k: Int, sc: SparkContext):  RDD[(Int,Int)] = {
    var i = x

    var row: List[(Int, Int)] = List()
    while(i <= k) {

      if(i == k){
        row = row:+(i,0)
      }
      else {
        row = row:+(i, i+1)
      }
      i+=1
    }

    val s = sc.parallelize(row)

    s
  }

  //Make the input based on the input parameter k and store it in input file input.csv
  def makeGraph(k: Int, input: String, sc: SparkContext):  RDD[(Int,Int)] = {

    var listOfRecords = sc.emptyRDD[(Int, Int)]

    for(i <- 1 to pow2(k) by k) {
      listOfRecords = listOfRecords union makeRow(i, i+k-1, sc)
    }

    listOfRecords
  }
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\npr.Pagerank <input dir> <output dir> <param-k>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Pagerank")
    val sc = new SparkContext(conf)

    val param = args{2}.toInt
    val graphRDD = makeGraph(param, args{0}, sc)

    //persist graphRDD
    graphRDD.persist()

    val zeroTuple = (0,0f)
    val zeroRow: List[(Int, Float)] = List()
    val dummyNodeRDD = sc.parallelize(zeroRow:+zeroTuple)

    val rankRDD = graphRDD.map(x =>  (x._1,1f/pow2(param)))

    val finalRankRDD = rankRDD union dummyNodeRDD

    //persist finalRankRDD
    finalRankRDD.persist()

//    println(finalRankRDD.collect().foreach(println))
//    println(graphRDD.collect().foreach(println))

    val contributions = graphRDD.join(finalRankRDD)
        .map(x => if (x._1 != 0) (x._1, x._2) else ())
//      .flatMap{ case(pagename,(links,rank)) =>
////      // if it is a dangling node output an empty list
////      if(links==0){
////        List()}
////      else{
////        // else we map the contribution to the outgoing node
////        //links.map(dest=>(dest,rank/size))
////      }}
////      // sum all the contributions from the node
////      .reduceByKey((x,y)=>(x+y))

    //val ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85*v)

    println(contributions.collect().foreach(println))

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
