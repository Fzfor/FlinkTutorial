package com.atguigu.apitest

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

/**
  * Create by fz on 2020/4/17
  */
object TransformTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val streamFromFile: DataStream[String] = env.readTextFile("F:\\workspaces\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    //1.  基本转换算子和简单聚合算子
    val dataStream: DataStream[SensorReading] = streamFromFile.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
    val aggsStream: DataStream[SensorReading] = dataStream.keyBy("id")
      //      .sum("temperature")
      //输出当前传感器最新的温度+10，而时间戳是上一次数据的时间+1
      .reduce((x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature + 10))

//    dataStream.print()

    //2.  多流转换算子
    //分流  split
    val splitStream: SplitStream[SensorReading] = dataStream.split(data =>
    if (data.temperature > 30) Seq("high") else Seq("low")
    )
    val high: DataStream[SensorReading] = splitStream.select("high")
    val low: DataStream[SensorReading] = splitStream.select("low")
    val all: DataStream[SensorReading] = splitStream.select("high","low")

//    high.print("high")
//    low.print("low")
//    all.print("all")

    //合并两条流
    val warning: DataStream[(String, Double)] = high.map(data => (data.id,data.temperature))
    val connectedStream: ConnectedStreams[(String, Double), SensorReading] = warning.connect(low)

    val coMapDataStream: DataStream[Product] = connectedStream.map(
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, "healthy")
    )

    //coMapDataStream.print()

    val unionStream: DataStream[SensorReading] = high.union(low)
//    unionStream.print("union")


    //函数类
    dataStream.filter( new MyFilter()).print()


    env.execute()
  }
}

class MyFilter() extends FilterFunction[SensorReading]{
  override def filter(t: SensorReading): Boolean = {
    t.id.startsWith("sensor_1")
  }
}
