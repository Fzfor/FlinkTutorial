package com.atguigu.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Create by fz on 2020/4/17
  */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.getConfig.setAutoWatermarkInterval(100)


    //source
    //val streamFromFile: DataStream[String] = env.readTextFile("F:\\workspaces\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    val streamFromFile: DataStream[String] = env.socketTextStream("hadoop102",7777)

    //Transform操作
    val dataStream = streamFromFile.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
//      .assignAscendingTimestamps(_.timestamp * 1000)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
    })



    //统计10秒内的最小温度
    //统计15秒内的最小温度,隔5秒输出一次
    val minTemPerWindowStream: DataStream[(String, Double)] = dataStream.map(data => (data.id, data.temperature))
      .keyBy(0)
      //.timeWindow(Time.seconds(15),Time.seconds(5)) //开时间窗口
      .timeWindow(Time.seconds(10)) //开时间窗口
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2))) //用reduce做增量聚合

    minTemPerWindowStream.print("min temp")

    dataStream.print("input data")


    dataStream.keyBy(_.id)
        .process(new MyProcess())


    env.execute()
  }

}

//class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading]{
//  val bound = 60000
//
//  var maxTs = Long.MinValue
//
//  override def getCurrentWatermark: Watermark = new Watermark(maxTs - bound)
//
//  override def extractTimestamp(t: SensorReading, l: Long): Long = {
//    maxTs = maxTs.max(t.timestamp * 1000)
//    t.timestamp * 1000
//  }
//}

class MyAssigner() extends AssignerWithPunctuatedWatermarks[SensorReading]{
  override def checkAndGetNextWatermark(t: SensorReading, l: Long): Watermark = new Watermark(l)

  override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp * 1000
}

class MyProcess() extends KeyedProcessFunction[String,SensorReading,String]{
  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {

  }
}
