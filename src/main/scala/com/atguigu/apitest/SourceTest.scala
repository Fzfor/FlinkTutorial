package com.atguigu.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.collection.immutable
import scala.util.Random

/**
  * 演示多种source
  * Create by fz on 2020/4/16
  */
object SourceTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //1.从自定义的集合中读取数据
    val stream1: DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    ))
    //stream1.print("stream1").setParallelism(1)

    //env.fromElements(1,2.89,"string").print()

    //2.从文件中读取数据
    val stream2: DataStream[String] = env.readTextFile("F:\\workspaces\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    //stream2.print("stream2").setParallelism(1)

    //3.从kafka中读取数据 可创建一个MyKafkautil工具类返回
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "hadoop102:9092")
    prop.setProperty("group.id", "consumer-group")
    prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("auto.offset.reset", "latest")

    val stream3: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),prop))

    //stream3.print("stream3").setParallelism(1)

    //4.  自定义source
    val stream4: DataStream[SensorReading] = env.addSource(new SensorSource)

    stream4.print("stream4").setParallelism(1)



    env.execute("sourct test")
  }
}

class SensorSource() extends SourceFunction[SensorReading]{
  //定义一个flag，表示数据源是否正常运行
  var running: Boolean = true

  //取消数据源的生成
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    //初始化一个随机数发生器
    val rand = new Random()

    //初始化定义一组传感器温度数据
    var curTemp: immutable.IndexedSeq[(String, Double)] = 1.to(10).map(
      i => ("sensor_" + i, 60 + rand.nextGaussian() * 20)
    )

    //用无限循环，产生数据流
    while (running) {
      //在前一次温度的基础上更新温度值
      curTemp = curTemp.map(
        t => (t._1,t._2+rand.nextGaussian())
      )

      //获取当前时间戳
      val curTime: Long = System.currentTimeMillis()

      curTemp.foreach(
        t => sourceContext.collect(SensorReading(t._1,curTime,t._2))
      )

      //设施时间间隔
      Thread.sleep(500)
    }

  }

  //正常生成数据
  override def cancel(): Unit = {
    running = false
  }
}

//温度传感器读数样例类
case class SensorReading(id: String, timestamp: Long, temperature: Double)
