package com.zhouhc.day3

import com.zhouhc.day2.SensorSource
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

//处理时间的 ，5s滚动窗口的 ，每个窗口温度的最少值
object ProcessTumblimgMinTempFunction {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setParallelism(1)

    val source = env.addSource(new SensorSource)

    val transfer = source.keyBy(_.id).window(TumblingProcessingTimeWindows.of(Time.seconds(30))).reduce((r1, r2) => if (r1.temperature > r2.temperature) r2 else r1)

    transfer.print()

    env.execute("ProcessTumblimgMinTempFunction")
  }

}
