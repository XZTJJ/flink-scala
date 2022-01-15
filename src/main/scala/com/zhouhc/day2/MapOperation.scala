package com.zhouhc.day2

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

//简单的Map数据
object MapOperation {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source = env.addSource(new SensorSource)

    source.map(new MyMapFunction).print()

    env.execute()

  }


  //输入和输出类型，复写Map
  class MyMapFunction extends MapFunction[SensorReading, String] {
    override def map(value: SensorReading): String = value.id
  }

}
