package com.zhouhc.day2

import org.apache.flink.streaming.api.scala._

//非常简单的消费数据
object SimpleConsumeFromSensorSource {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val source = env.addSource(new SensorSource)

    source.print()
    env.execute("ConsumeFromSensorSource")
  }

}
