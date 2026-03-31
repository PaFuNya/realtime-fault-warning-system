package org.example;

import redis.clients.jedis.Jedis;

public class RedisMockDataApp {
    public static void main(String[] args) {
        // 连接到你的 Redis 服务器
        String redisHost = "100.126.226.67";
        int redisPort = 6379;

        System.out.println("正在连接 Redis 服务器: " + redisHost + ":" + redisPort);

        try (Jedis jedis = new Jedis(redisHost, redisPort)) {
            // 如果 Redis 有密码，请取消注释下一行并填入密码
            // jedis.auth("your_password");

            // 验证连接是否成功
            System.out.println("Redis 连接成功: " + jedis.ping());

            // 录入不同机器的标准电流数据
            // 假设我们有 5 台机器
            System.out.println("开始写入标准工艺参数(电流)...");

            jedis.set("std_current:Machine_001", "30.0");
            System.out.println("已写入: std_current:Machine_001 = 30.0");

            jedis.set("std_current:Machine_002", "35.0");
            System.out.println("已写入: std_current:Machine_002 = 35.0");

            jedis.set("std_current:Machine_003", "40.0");
            System.out.println("已写入: std_current:Machine_003 = 40.0");

            jedis.set("std_current:Machine_004", "45.0");
            System.out.println("已写入: std_current:Machine_004 = 45.0");

            jedis.set("std_current:Machine_005", "50.0");
            System.out.println("已写入: std_current:Machine_005 = 50.0");

            System.out.println("所有模拟数据录入完成！");

            // 验证一下是否真的写进去了
            System.out.println("\n--- 验证读取 ---");
            System.out.println("Machine_001 的标准电流是: " + jedis.get("std_current:Machine_001"));
            System.out.println("Machine_005 的标准电流是: " + jedis.get("std_current:Machine_005"));

        } catch (Exception e) {
            System.err.println("连接或写入 Redis 时发生错误！");
            e.printStackTrace();
        }
    }
}
