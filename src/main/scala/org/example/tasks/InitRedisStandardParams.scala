package org.example.tasks

import redis.clients.jedis.Jedis

/** 辅助脚本：初始化 Redis 中的设备标准工艺参数 供 Task 18 (实时工艺参数偏离监控) 联查使用。
  */
object InitRedisStandardParams {
  def main(args: Array[String]): Unit = {
    // 连接到 Linux 服务器上的 Redis (假设你的 Redis 跑在 master 或默认的 127.0.0.1 映射)
    // 注意：如果你的 Redis 在虚拟机里，请将 127.0.0.1 替换为 master
    val redisHost = "master"
    val redisPort = 6379

    println(s"正在连接 Redis: $redisHost:$redisPort ...")
    val jedis = new Jedis(redisHost, redisPort)

    try {
      // 根据真实生成的 machine_id 列表批量写入标准参数
      // 这些是你模拟数据中真实存在的设备 ID (如 101, 102, ..., 118)
      val machineIds = List(
        "101",
        "102",
        "103",
        "104",
        "105",
        "112",
        "114",
        "115",
        "116",
        "117",
        "118"
      )

      machineIds.foreach { id =>
        // 进一步调整标准值，使其极其贴近模拟数据的真实中心值，从而几乎不触发报警。
        // 根据你刚才的数据日志 (30.05 ~ 30.92)，模拟数据的电流实际上非常集中在 30 附近。
        // 将标准值设为 30.0，那么 10% 的报警阈值就是 > 33.0 或者 < 27.0。
        // 这样正常数据就不会报警了，只有真正发生极端偏差时才会写入。
        jedis.hset(s"std_params:$id", "current", "30.0")
        jedis.hset(s"std_params:$id", "speed", "3000.0") // 假设转速正常在 3000 左右
      }

      println(s"✅ 成功向 Redis 写入 ${machineIds.length} 台设备的标准工艺参数！")

      // 验证读取其中一台
      val current101 = jedis.hget("std_params:101", "current")
      println(s"验证读取 -> 设备 101 的标准电流: $current101")

    } catch {
      case e: Exception =>
        println(s"❌ 写入 Redis 失败，请检查 Redis 是否启动或 IP 是否正确: ${e.getMessage}")
    } finally {
      jedis.close()
    }
  }
}
