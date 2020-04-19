package com.atguigu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
/**
  * 流处理word count 程序
  * Create by fz on 2020/4/16
  */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    //创建流处理的执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //创建一个socket文本流
    val dataStream: DataStream[String] = env.socketTextStream(host,port)

    //对每条数据进行处理
    val wordCountDataStream: DataStream[(String, Int)] = dataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    wordCountDataStream.print()

    //启动executor
    env.execute()
  }
}
