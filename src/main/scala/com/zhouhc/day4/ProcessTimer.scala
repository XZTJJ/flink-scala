package com.zhouhc.day4

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.sql.Timestamp

//通过为 process time 设置一个定时器，就可触发回掉
//注意，每个数据只能有一个定时器，只要系统时间超过定时器的时间戳，就会触发 ，和窗口一样，左闭右开
//因为是 process time，所以只要关注机器时间，有没有超过输入时，10s就可以了
object ProcessTimer {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //注意这里分流以后并没有设置一个window，通过KeyProcessFunction的方式触发回掉，这里使用的是process时间，第二个元素无用
    val stream = env.socketTextStream("localhost", 9999, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000L)
      }).keyBy(_._1).process(new MyKeyFun)

    stream.print()

    env.execute("ProcessTimer")
  }

  class MyKeyFun extends KeyedProcessFunction[String, (String, Long), String] {

    //这里需要注册一个定时器，每一个元素到来时，都会触发该方法
    override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), String]#Context, out: Collector[String]): Unit = {
      //注意，获取当前系统时间 + 10s 作为触发器
      val triggerTime = ctx.timerService().currentProcessingTime()+ 10 * 1000L
      //注册定时器
      ctx.timerService().registerProcessingTimeTimer(triggerTime)
    }

    //当注册的定时器达到时，就会触发 onTimer 方法里面的逻辑，并且  processElement 和 onTimer 不能同时被调用，只能有一个先调用
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Long), String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect(s"""定时器被触发了，当前的时间戳为${new Timestamp(timestamp)}""")
    }
  }
}
