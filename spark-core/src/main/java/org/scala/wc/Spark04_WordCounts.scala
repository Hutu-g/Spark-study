package org.scala.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @Author: hutu
 * @Date: 2024/2/29 18:05
 */
object Spark04_WordCounts {
  def main(args: Array[String]): Unit = {
    //连接
    val wordCount: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(wordCount)
//    wordCount1(sc)
    wordCount2(sc)

    //关闭连接
    sc.stop();
  }
  def wordCount1(sc:SparkContext): Unit ={
    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello world", "hello python"))
    val flatMap: RDD[String] = rdd.flatMap(_.split(" "))
    val groupRdd: RDD[(String, Iterable[String])] = flatMap.groupBy(word => word)
    val wordCount: RDD[(String, Int)] = groupRdd.mapValues(iter => iter.size)
    wordCount.collect.foreach(println)
  }

  def wordCount2(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello world", "hello python"))
    val flatMap: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRdd: RDD[(String, Int)] = flatMap.map((_, 1))

    val group: RDD[(String, Iterable[Int])] = mapRdd.groupByKey()
    val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
    wordCount.collect.foreach(println)
  }

  def wordCount3(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello world", "hello python"))
    val flatMap: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRdd: RDD[(String, Int)] = flatMap.map((_, 1))
    val wordCount: RDD[(String, Int)] = mapRdd.reduceByKey(_ + _)
    wordCount.collect.foreach(println)
  }

  def wordCount4(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello world", "hello python"))
    val flatMap: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRdd: RDD[(String, Int)] = flatMap.map((_, 1))
    val wordCount: RDD[(String, Int)] = mapRdd.aggregateByKey(0)(_ + _, _ + _)
    wordCount.collect.foreach(println)
  }

  def wordCount5(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello world", "hello python"))
    val flatMap: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRdd: RDD[(String, Int)] = flatMap.map((_, 1))
    val wordCount: RDD[(String, Int)] = mapRdd.foldByKey(0)(_ + _)
    wordCount.collect.foreach(println)
  }

  def wordCount6(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello world", "hello python"))
    val flatMap: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRdd: RDD[(String, Int)] = flatMap.map((_, 1))
    val wordCount: RDD[(String, Int)] = mapRdd.combineByKey(
      v => v,
     (x: Int, y) => x + y, (x: Int, y: Int) => x + y
    )
    wordCount.collect.foreach(println)
  }

  def wordCount7(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("hello spark", "hello world", "hello python"))
    val flatMap: RDD[String] = rdd.flatMap(_.split(" "))
    val wordCount: collection.Map[String, Long] = flatMap.countByValue()
    println(wordCount)
  }
  
}
