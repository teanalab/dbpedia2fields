package edu.wayne.dbpedia2fields

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

/**
  * Created by fsqcds on 6/24/16.
  */
object Predicates {
  def main(args: Array[String]): Unit = {
    val pathToTriples = args(0)
    val pathToOutput = args(1)

    val conf = new SparkConf().setAppName("TopPredicates")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val triples = sc.textFile(pathToTriples)

    val topPreds = triples.flatMap { line =>
      Try {
        line.split(" ")(1)
      }.toOption
    }.map(word => (word, 1))
      .reduceByKey(_ + _, 1) // 2nd arg configures one task (same as number of partitions)
      .map(item => item.swap) // interchanges position of entries in each tuple
      .sortByKey(false, 1) // 1st arg configures ascending sort, 2nd arg configures one task
      .map(item => s"${item._2}\t${item._1}")
      .saveAsTextFile(pathToOutput)
  }
}
