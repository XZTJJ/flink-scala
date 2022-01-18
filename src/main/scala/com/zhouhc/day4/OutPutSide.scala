package com.zhouhc.day4

import com.zhouhc.day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

//flink的分流机制，侧边输出，侧边输出的可以有多个，并且允许不同侧板输出类型不一样
object OutPutSide {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //在processFunction中定义侧边输出
    val stream = env.addSource(new SensorSource).process(new MyProcessFunction)
    //正常的打印
    stream.print();
    //侧边打印
    stream.getSideOutput(new OutputTag[String]("low-temp-alarm")).print()

    env.execute("OutPutSide")
  }

  class MyProcessFunction extends ProcessFunction[SensorReading, SensorReading] {
    //定义一个侧边输出
    lazy val outPutTag = new OutputTag[String]("low-temp-alarm")

    override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      //侧边输出
      if (value.temperature < 0) {
        ctx.output(outPutTag, s"""${value.id}进行低温报警，温度为${value.temperature}""")
      }
      out.collect(value)
    }
  }
}
