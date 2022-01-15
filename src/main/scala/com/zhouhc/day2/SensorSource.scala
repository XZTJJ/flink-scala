package com.zhouhc.day2

import org.apache.flink.streaming.api.functions.source._
import java.time._
import scala.util.Random

//无限的数据源
class SensorSource extends RichParallelSourceFunction[SensorReading] {
  var running: Boolean = true;

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {

    var rand = new Random
    //产生设备号 和温度
    var curFTemp = (1 to 10).map(
      t => ("Sensor_" + t, rand.nextGaussian * 20)
    )

    while (running) {
      //获取每次温度都不一样
      curFTemp = curFTemp.map(t => (t._1, t._2 + (rand.nextGaussian * 0.5)))
      //获取时间
      var curTime = ZonedDateTime.now(ZoneOffset.of("+8")).toInstant.toEpochMilli
      //每一条数据都发送
      curFTemp.foreach(t => ctx.collect(SensorReading(t._1, curTime, t._2)))
      //线程休眠
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = running = false
}
