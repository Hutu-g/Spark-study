package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 *
 * @Author: hutu
 * @Date: 2024/5/8 10:05
 */
object Spark_sql_Test1 {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop01")
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate() //导入隐式转换 对象里的内容
    spark.sql("use spark_sql")
    //第一张表
    spark.sql(
      """
        |select
        |  *
        |from(
        |    select
        |    * ,
        |    rank() over(partition by area order by clickCnt desc) as rank
        |  from(
        |      select
        |      area,
        |      product_name,
        |      count(*) as clickCnt
        |    from (
        |        select
        |          a.*,
        |          p.product_name,
        |          c.area,
        |          c.city_name
        |      from user_visit_action a
        |      join product_info p on a.click_product_id = p.product_id
        |      join city_info c on a.city_id = c.city_id
        |      where a.click_product_id > -1
        |    ) t1 group by area,product_name
        |  ) t2
        |) t3 where rank <=3
        |""".stripMargin).show()
    spark.close()
  }

}
