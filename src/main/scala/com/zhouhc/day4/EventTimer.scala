package com.zhouhc.day4

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

//通过为 event time 设置一个定时器，就可触发回掉
//注意，每个水印只能有一个定时器，只要水印超过定时器的时间戳，就会触发 ，和窗口一样，左闭右开
//当输入 1时，触发最小的水印， 因为有10s相加，所以只有超过 11的时候，才会触发1的定时器。因此 12 会触发1的定时器，同理 23 会触发 12 的定时器
//a 1
//a 12
//a 20
//a 23
//a 31
//a 34
object EventTimer {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //注意这里分流以后并没有设置一个window，通过KeyProcessFunction的方式触发回掉，这里使用的是事件时间，
    //并且该事件时间需要升序，而且指定的是第二个元素为事件时间
    val stream = env.socketTextStream("localhost", 9999, '\n')
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong * 1000L)
      }).assignAscendingTimestamps(_._2).keyBy(_._1).process(new MyKeyFun)

    stream.print()

    env.execute("EventTimer")
  }

  class MyKeyFun extends KeyedProcessFunction[String, (String, Long), String] {

    //这里需要注册一个定时器，每一个元素到来时，都会触发该方法
    override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), String]#Context, out: Collector[String]): Unit = {
      //注意，这里获取了当前 水印时间，并且 + 10s作为触发的时间
      val triggerTime = ctx.timerService().currentWatermark() + 10 * 1000L
      //注册定时器
      ctx.timerService().registerEventTimeTimer(triggerTime)
    }

    //当注册的定时器达到时，就会触发 onTimer 方法里面的逻辑，并且  processElement 和 onTimer 不能同时被调用，只能有一个先调用
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Long), String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect(s"""定时器被触发了，当前的时间戳为${timestamp}""")
    }
  }
}
