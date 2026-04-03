package edu.cmp26

import org.apache.spark.sql.SparkSession

object ReadMl {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val ss = SparkSession.builder().appName("ReadMl")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()
    val str=
      s"""
        select * from default.student
        """.stripMargin
    val df = ss.sql(str)
    df.show()
    ss.close()
  }

}
