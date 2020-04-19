package com.atguigu.apitest.sinkTest

import java.util.Properties

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
  * Create by fz on 2020/4/17
  */
object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //source
    //val streamFromFile: DataStream[String] = env.readTextFile("F:\\workspaces\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

//    从kafka中读取数据 可创建一个MyKafkautil工具类返回
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "hadoop102:9092")
    prop.setProperty("group.id", "consumer-group")
    prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("auto.offset.reset", "latest")

    val streamFromFile: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),prop))


    //Transform操作
    val dataStream: DataStream[String] = streamFromFile.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString //转成string，方便序列化输出
    })

    //sink
    dataStream.addSink(new FlinkKafkaProducer011[String]("hadoop102:9092","sinkTest",new SimpleStringSchema()))

    dataStream.print()

    env.execute("kafka sink test")

  }

}
