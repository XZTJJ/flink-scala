package com.zhouhc.day1

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

//获取静态数据
object WordCountFromStatic {

  def main(args: Array[String]): Unit = {
    //获取运行时的环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)

    //添加数据源
    val source = env.fromElements("hello world","hello world","hello test is a word")
    //设置处理过程
    //分解成单词
    val transfer = source.flatMap(str => str.split(" "))
      //转成一个元祖
      .map(word => (word,1))
      //shffule
      .keyBy(innerWordCount => innerWordCount._1)
      .sum(1)
    //简单的打印
    transfer.print()

    env.execute("WordCountFromStatic")
  }
}
