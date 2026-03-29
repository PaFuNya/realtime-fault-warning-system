package order.ods

import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.io.BufferedWriter
import scala.util.Try
import org.json4s.JsonDSL._
import org.json4s.Extraction._

object JsonToCsvConverter {
  implicit val formats: DefaultFormats.type = DefaultFormats

  def main(args: Array[String]): Unit = {
    // 假设你的JSON文件路径是 "access.json"
    val jsonFilePath = "access.json"
    // 输出CSV文件路径
    val csvFilePath = "output.csv"

    // 检查文件是否存在
    if (!Files.exists(Paths.get(jsonFilePath))) {
      println(s"Error: File not found at path $jsonFilePath")
      return
    }

    // 读取文件内容
    val jsonString = new String(Files.readAllBytes(Paths.get(jsonFilePath)))

    // 将JSON字符串解析为JValue对象
    Try(parse(jsonString)) match {
      case scala.util.Success(json) =>
        // 如果你的JSON文件包含多个JSON对象，请确保以适当的方式解析（例如，作为数组）
        val records = json match {
          case JArray(records) => records
          case record => List(record)
        }

        // 写入CSV文件
        val writer = Files.newBufferedWriter(Paths.get(csvFilePath), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
        try {
          // 写入CSV头
          writer.write("recordType,uid,device,ip,,timestamp\n")

          // 转换每条记录并写入CSV文件
          records.foreach { record: JValue =>
            val csvLine = convertToJsonTarget(compact(render(record)))
            if (csvLine.nonEmpty) {
              writer.write(csvLine + "\n")
            }
          }
        } finally {
          writer.close()
        }
      case scala.util.Failure(e) =>
        println(s"Error parsing JSON: ${e.getMessage}")
    }
  }

  def convertToJsonTarget(jsonString: String): String = {
    // 解析单个JSON对象
    Try(parse(jsonString)) match {
      case scala.util.Success(json) =>
        // 提取字段，并处理可能的缺失字段
        val eventType = (json \ "event").extractOpt[String].getOrElse("")
        val uid = (json \ "uid").extractOpt[String].getOrElse("")
        val device = (json \ "device").extractOpt[String].getOrElse("")
        val timestamp = (json \ "time").extractOpt[Long].getOrElse(0L)
        val formattedTime = Try(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(new java.util.Date(timestamp / 1000))).getOrElse("")
        val ip = (json \ "ip").extractOpt[String].getOrElse("")

        // 根据eventType决定输出格式
        eventType match {
          case "startup" | "browse" =>
            val recordType = if (eventType == "startup") "S" else "B"
            s"""$recordType,"$uid","$device","$ip",,,"$formattedTime"""" // 修正了这里的引号
          case _ => ""
        }
      case scala.util.Failure(e) =>
        println(s"Error converting JSON: ${e.getMessage}")
        ""
    }
  }
}
