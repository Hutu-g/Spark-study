package org.scala.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 转换算子 将 List("Hello", "hive", "hbase", "Hadoop")根据单词首写字母进行分组。
 *
 * @Author: hutu
 * @Date: 2024/3/23 13:29
 */
object tran_06_groupBy_test {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello","Spark","Scala","Hadoop"),2)

    val groupRdd: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))

    groupRdd.collect().foreach(println)
    sc.stop()
  }

}
