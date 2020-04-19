package com.atguigu.apitest.sinkTest

import java.sql.{Connection, Driver, DriverManager, PreparedStatement}

import com.atguigu.apitest.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
  * Create by fz on 2020/4/17
  */
object JDBCSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //source
    val streamFromFile: DataStream[String] = env.readTextFile("F:\\workspaces\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    //Transform操作
    val dataStream = streamFromFile.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    //sink
    dataStream.addSink( new MyJdbcSink())

    env.execute("jdbc sink test")
  }

}

class MyJdbcSink() extends RichSinkFunction[SensorReading]{
  //定义sql连接，预编译器
  var con: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  //初始化，创建连接和预编译语句
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    con = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test","root","zb346224813")

    insertStmt = con.prepareStatement("INSERT INTO temperature (sensor, temp) VALUES (?,?)")
    updateStmt = con.prepareStatement("UPDATE temperature SET temp = ? WHERE sensor = ?")
  }

  //调用连接，执行sql
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    //执行更新语句
    updateStmt.setDouble(1,value.temperature)
    updateStmt.setString(2,value.id)
    updateStmt.execute()

    //如果update没有查到数据，那么执行插入语句
    if (updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1,value.id)
      insertStmt.setDouble(2,value.temperature)
      insertStmt.execute()
    }
  }

  //关闭时做清理工作
  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    con.close()
  }
}