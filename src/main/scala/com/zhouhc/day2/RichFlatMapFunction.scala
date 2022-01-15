package com.zhouhc.day2

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


//富函数的联系
object RichFlatMapFunction {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //数据源
    val source = env.fromElements((1, 1), (2, 2), (3, 3), (4, 4), (5, 5))
    //转换操作
    val transfer = source.flatMap(new MyRichFlatMapOperation)

    transfer.print()
    env.execute("RichFlatMapFunction")

  }


  class MyRichFlatMapOperation extends RichFlatMapFunction[(Int, Int), (Int, Int)] {

    override def open(parameters: Configuration): Unit = {
      println("生命周期开始")
    }

    override def flatMap(value: (Int, Int), out: Collector[(Int, Int)]): Unit = {
      if (value._1 % 2 == 0) {
        out.collect(value);
        println("加1")
      }
    }

    override def close(): Unit = {
      println("生命周期结束")
    }
  }

}
