package com.atguigu.apitest.sinkTest

import java.util

import com.atguigu.apitest.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
  * Create by fz on 2020/4/17
  */
object EsSinkTest {
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

    //sink
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop102",9200))

    //创建一个ESSink的builder
    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          println("saving data: " + t.toString)

          //包装成一个Map或者JSONObject
          val map = new util.HashMap[String, String]()
          map.put("sensor_id", t.id)
          map.put("temperature", t.temperature.toString)
          map.put("ts", t.timestamp.toString)

          //创建index request，准备发送数据
          val indexRequest: IndexRequest = Requests.indexRequest()
            .index("sensor")
            .`type`("readingdata")
            .source(map)

          //利用requestIndexer发送请求，写入数据
          requestIndexer.add(indexRequest)

          println("data saved")
        }
      }
    )

    dataStream.addSink(esSinkBuilder.build())

    env.execute("es sink test")

  }
}
