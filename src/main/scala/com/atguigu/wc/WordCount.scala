package com.atguigu.wc

import org.apache.flink.api.scala._

/**
  * Create by fz on 2020/4/16
  */
//批处理word count 程序
object WordCount {
  def main(args: Array[String]): Unit = {
    //创建一个执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val inputPath = "F:\\workspaces\\IdeaProjects\\FlinkTutorial\\src\\main\\resources\\hello.txt"
    val inputDataSet: DataSet[String] = env.readTextFile(inputPath)

    //切分数据得到word，然后再按word做分组聚合
    val wordCountDataSet: AggregateDataSet[(String, Int)] = inputDataSet.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    wordCountDataSet.print()

  }
}
