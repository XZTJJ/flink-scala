package com.zhouhc.day3

import com.zhouhc.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

//处理时间的 ，5s滚动窗口的 ，每个窗口温度的平均值，注意 AggregateFunction 只是保留累加器，非常节省空间
//增量聚合函数
object ProcessTumblingAvgTempAgg {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new SensorSource)

    stream.keyBy(_.id).window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .aggregate(new MyAGG).print()

    env.execute("ProcessTumblingAvgTempAgg")
  }

  /**
   * AggregateFunction需要定义三种类型，一种是输入类型，一种累机器类型，一种是输出类型
   * 输入类型是 : SensorReading
   * 累机器类型: String是传感器id, Long是到达的总数， Double是达到的温度总和
   * 输出类型 ：String时传感器id, Double是评价温度
   */
  class MyAGG extends AggregateFunction[SensorReading, (String, Long, Double), (String, Double)] {
    //初始化累加器
    override def createAccumulator(): (String, Long, Double) = ("", 0L, 0.0)

    //累机器的累加规则
    override def add(value: SensorReading, accumulator: (String, Long, Double)): (String, Long, Double) = {
      //传感器id ， 计数 和总数 分别要加上对应的指
      (value.id, accumulator._2 + 1, accumulator._3 + value.temperature)
    }

    //获取结果，结果就是  平均值
    override def getResult(accumulator: (String, Long, Double)): (String, Double) = (accumulator._1, accumulator._3 / accumulator._2)

    //只有在 event time的时候才会用到，主要的用途就是，累积器之间的合并规则
    override def merge(a: (String, Long, Double), b: (String, Long, Double)): (String, Long, Double) = (a._1, a._2 + b._2, a._3 + b._3)
  }
}
