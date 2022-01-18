package com.zhouhc.day4

import com.zhouhc.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

//可以同时 两条流的 connection的底层操作，对每一条流的元素处理 类似 processFunction
object ConProcessOp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //定义两个不同的数据源
    val source1 = env.addSource(new SensorSource).keyBy(_.id)
    val source2 = env.fromElements(("Sensor_1", 10 * 1000L), ("Sensor_5", 5 * 1000L))

    //两个流进行连接操作
    val result = source1.connect(source2).keyBy(_.id, _._1).process(new MyConProcessFunction)

    result.print();
    env.execute("ConProcessOp")
  }

  class MyConProcessFunction extends CoProcessFunction[SensorReading, (String, Long), SensorReading] {
    //每个key流都会有一个属于自己的 upStreamValue 属性，的
    lazy val upStreamValue = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("isupStream", Types.of[Boolean]))

    override def processElement1(value: SensorReading, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      if (upStreamValue.value()) {
        out.collect(value)
      }
    }

    override def processElement2(value: (String, Long), ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context, out: Collector[SensorReading]): Unit = {
      upStreamValue.update(true)
      val ts = ctx.timerService().currentProcessingTime() + value._2
      ctx.timerService().registerProcessingTimeTimer(ts)
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext, out: Collector[SensorReading]): Unit = {
      upStreamValue.clear()
    }

  }
}
