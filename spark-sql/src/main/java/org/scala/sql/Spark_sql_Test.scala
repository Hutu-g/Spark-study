package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 *
 * @Author: hutu
 * @Date: 2024/5/8 10:05
 */
object Spark_sql_Test {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop01")
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate() //导入隐式转换 对象里的内容
    spark.sql("use spark_sql")
    spark.sql("show tables").show()
    //第一张表
    spark.sql(
      """
        |CREATE TABLE `user_visit_action`(
        | `date` string,
        | `user_id` bigint,
        | `session_id` string,
        | `page_id` bigint,
        | `action_time` string,
        | `search_keyword` string,
        | `click_category_id` bigint,
        | `click_product_id` bigint,
        | `order_category_ids` string,
        | `order_product_ids` string,
        | `pay_category_ids` string,
        | `pay_product_ids` string,
        | `city_id` bigint)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)
    spark.sql(
      """
        |load data local inpath 'datas/spark_sql/user_visit_action.txt' into table spark_sql.user_visit_action
        |""".stripMargin)
    //第二张表
    spark.sql(
      """
        |CREATE TABLE `product_info`(
        | `product_id` bigint,
        | `product_name` string,
        | `extend_info` string)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)
    spark.sql(
      """
        |load data local inpath 'datas/spark_sql/product_info.txt' into table spark_sql.product_info
        |""".stripMargin)
    //第三张表
    spark.sql(
      """
        |CREATE TABLE `city_info`(
        | `city_id` bigint,
        | `city_name` string,
        | `area` string)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)
    spark.sql(
      """
        |load data local inpath 'datas/spark_sql/city_info.txt' into table spark_sql.city_info
        |""".stripMargin)
    spark.sql("select * from city_info").show()
    spark.close()
  }

}
