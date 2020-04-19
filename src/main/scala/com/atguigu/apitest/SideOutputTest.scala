package com.atguigu.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Create by fz on 2020/4/18
  */
object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    //source

    val streamFromFile: DataStream[String] = env.socketTextStream("hadoop102",7777)

    //Transform操作
    val dataStream = streamFromFile.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
      })

    val processedStream: DataStream[SensorReading] = dataStream.process(new FreezingAlert())

    processedStream.print("processed data")

    processedStream.getSideOutput(new OutputTag[String]("freezing alert")).print("alert data")


    env.execute()
  }

}


//冰点报警，如果小于32F，输出报警信息到侧输出流
class FreezingAlert() extends ProcessFunction[SensorReading,SensorReading]{
  lazy val alertOutput = new OutputTag[String]("freezing alert")

  override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
    if (i.temperature < 32) {
      context.output(alertOutput, "freezing alert for  " + i.id)
    } else{
      collector.collect(i)
    }
  }
}
