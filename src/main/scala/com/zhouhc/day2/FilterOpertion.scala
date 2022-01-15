package com.zhouhc.day2

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

//简单的过滤数据
object FilterOpertion {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.addSource(new SensorSource)
    source.filter(new MyFilterFunction).print()
    env.execute("FilterOpertion")

  }


  class MyFilterFunction extends FilterFunction[SensorReading] {
    override def filter(value: SensorReading): Boolean = value.id.equals("Sensor_3") && value.temperature > 0
  }

}
