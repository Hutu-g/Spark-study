package org.scala.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @Author: hutu
 * @Date: 2024/2/29 18:05
 */
object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    //连接
    val wordCount: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(wordCount)

    //业务
    //1.读取文件
    //
    val lines: RDD[String] = sc.textFile("datas")
    //扁平化操作
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //分组
    val wordsGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    val wordToCount =  wordsGroup.map{
      case (word,list) =>{
        (word,list.size)
    }
    }

    val array: Array[(String, Int)] = wordToCount.collect()

    array.foreach(println)
    //关闭连接
    sc.stop();
  }

}
