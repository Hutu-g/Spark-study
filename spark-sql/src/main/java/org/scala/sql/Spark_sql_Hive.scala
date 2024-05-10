package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 *
 * @Author: hutu
 * @Date: 2024/5/8 10:05
 */
object Spark_sql_Hive {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate() //导入隐式转换 对象里的内容
    //1.cp hive-site.xml
    //2.启用hive支持
    //3.增加mysql驱动
    spark.sql("show tables").show()

    spark.close()


  }

}
