package com.zhouhc.day3

import com.zhouhc.day2.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//处理时间的 ，5s滚动窗口的 ，每个窗口温度的平均值，注意 ProcessWindowFunction 会保留每个窗口的所有元素，非常浪费内存
//全窗口聚合的函数
object ProcessTunmblingAvgTempFunction {

  case class AvgTemp(id: String, avgTemp: Double, windowStart: Long, windowEnd: Long);

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val source = env.addSource(new SensorSource)
    val transfer = source.keyBy(_.id).window(TumblingProcessingTimeWindows.of(Time.seconds(20))).process(new MyProcessWindowFunction)

    transfer.print()
    env.execute("ProcessTunmblingAvgTempFunction")
  }


  class MyProcessWindowFunction extends ProcessWindowFunction[SensorReading, AvgTemp, String, TimeWindow] {

    override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[AvgTemp]): Unit = {
      var count = elements.size
      var sum = 0.0
      for (r <- elements) {
        sum = sum + r.temperature
      }
      val windownStart = context.window.getStart
      val windowEnd = context.window.getEnd
      out.collect(AvgTemp(key, sum / count, windownStart, windowEnd))
    }
  }
}
