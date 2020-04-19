package com.atguigu.apitest.sinkTest

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * Create by fz on 2020/4/17
  */
object  RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //source
    val streamFromFile: DataStream[String] = env.readTextFile("F:\\workspaces\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    //Transform操作
    val dataStream= streamFromFile.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("hadoop102")
      .setPort(6379)
      .build()

    //sink
    dataStream.addSink(new RedisSink(conf,new MyRedisMapper))

    env.execute()
  }
}


class MyRedisMapper() extends RedisMapper[SensorReading]{
  //定义保存数据到redis的命令
  override def getCommandDescription: RedisCommandDescription = {
    //把传感器id和温度值保存成哈希表 HSET key field value
    new RedisCommandDescription(RedisCommand.HSET,"sensor_temperature")
  }

  //定义保存到redis的value
  override def getKeyFromData(t: SensorReading): String = t.temperature.toString

  //定义保存到redis的key
  override def getValueFromData(t: SensorReading): String = t.id
}
