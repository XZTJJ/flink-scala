package com.zhouhc.day5

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

//a 800
//a 900
//a 1200
//a 3800
//a 12000

//自定义的触发器， 在窗口中的每个整数秒都会触发计算逻辑， 同样trigger会调用 process()中的逻辑，没有的话就是默认的逻辑
//flink默认处理调用窗口的关闭逻辑的，用的比较少
object WindowTrigger {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.socketTextStream("localhost", 9999, '\n')
      //注意这里直接写的是毫秒数
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toLong)
      })
      //赋予时间字段，分流，开窗， 触发器， 处理逻辑
      .assignAscendingTimestamps(_._2).keyBy(_._1).window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .trigger(new SecondInterverTrigger).process(new MyProcess)

    stream.print()
    env.execute("WindowTrigger")
  }

  class SecondInterverTrigger extends Trigger[(String, Long), TimeWindow] {
    //没来一个元素会调用的方式
    override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      //判断是否为第一个元素
      val isFirst = ctx.getPartitionedState(new ValueStateDescriptor[Boolean]("first-ele", Types.of[Boolean]))
      //如果是第一个元素，就注册一个自然秒的 timer
      if (!isFirst.value()) {
        println(s"""第一个元素到来，默认的初始watermark是${ctx.getCurrentWatermark}""")
        val ts = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
        println(s"""第一个元素注册的eventTime timer为 ${ts}, 它所属的窗口的结束时间${window.getStart}~${window.getEnd}""")
        ctx.registerEventTimeTimer(ts)
        //同时需要注册一个窗口结束的 timer
        ctx.registerEventTimeTimer(window.getEnd)
        isFirst.update(true)
      }
      TriggerResult.CONTINUE
    }

    // process time的trigger处理逻辑
    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    //需要注意，要在这里处理上面定义的timer
    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      println("eventTime被触发了，触发的时间为" + time)
      //表示窗口已经结束，可以清理了
      if (ctx.getCurrentWatermark >= window.getEnd) {
        println(s"""窗口${window.getStart} ~ ${window.getEnd} 要被销毁了""")
        TriggerResult.FIRE_AND_PURGE
      } else {
        val ts = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
        //需要保证不会两次触发边缘逻辑
        if (ts < window.getEnd) {
          println(s"""其他元素注册的eventTime timer为 ${ts}, 它所属的窗口的结束时间${window.getStart}~${window.getEnd}""")
          ctx.registerEventTimeTimer(ts)
        }
        TriggerResult.FIRE
      }
    }

    //清理逻辑
    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
      //注意这是一个单利模式
      val isFirst = ctx.getPartitionedState(new ValueStateDescriptor[Boolean]("first-ele", Types.of[Boolean]))
      isFirst.clear()
    }
  }

  //自定义类,处理类
  class MyProcess extends ProcessWindowFunction[(String, Long), String, String, TimeWindow] {
    //没来一个元素都会调用的方式
    override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
      out.collect(s"""当前窗口有${elements.size}个元素，并且窗口结束时间为 ${context.window.getEnd}""")
    }
  }
}
