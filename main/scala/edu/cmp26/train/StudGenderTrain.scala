package edu.cmp26.train

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object StudGenderTrain {
  def main(args: Array[String]): Unit = {
    val sparkConf:SparkConf=new SparkConf().setAppName("StudGenderTrain").setMaster("local[*]")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //1 查询数据
    var sql=
      s""" select uid,
         | case hair when '长发' then 101
         | when '短发' then 102
         | when '板寸' then 103 end as hair,
         | height,
         | case skirt when '是' then 11
         | case age when '80后' then 80
         | when '90后' then 90
         | when '00后' then 100 end as age ,
         | gender
         | from user_profile2077.student
         """.stripMargin
    sparkSession.sql("select")
    //2 切分数据 分成 训练集和测试集  8:2   7:3

    //3 创建mypipeline

    // 4 进行训练
    //5 进行预测
    //6 打印预测结果
  }
}
