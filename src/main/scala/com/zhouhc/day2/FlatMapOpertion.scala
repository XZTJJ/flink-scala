package com.zhouhc.day2

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

//FlatMap的简答测试，FlatMap功能非常强大
object FlatMapOpertion {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val source = env.addSource(new SensorSource)
    source.flatMap(new MyFlatMapFunction).print()
    env.execute("FlatMapOpertion")
  }

  class MyFlatMapFunction extends FlatMapFunction[SensorReading, SensorReading] {
    override def flatMap(value: SensorReading, out: Collector[SensorReading]): Unit = {
      if (value.id.equals("Sensor_3") && value.temperature > 0)
        out.collect(value)
    }

  }


}
