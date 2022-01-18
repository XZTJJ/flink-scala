package com.zhouhc.day4

import com.zhouhc.day2.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

//检测3s内温度连续上升，则触发报警
//使用的 process time ，所以和事件时间没有关系了
object UpdateTemp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //分流，不用开窗了，使用的是process time, 运行很久也没有看到升温的结果，所以注释掉，使用下面的程序测试
    //    val stream = env.addSource(new SensorSource)
    //      .keyBy(_.id).process(new MyKeyProcess)

    //手动测试场景，如果自动太难出现就手动模拟一个
    val stream = env.socketTextStream("localhost", 9999, '\n').map(line => {
      val arr = line.split(" ")
      SensorReading(arr(0), 1000L, arr(1).toDouble)
    }).keyBy(_.id).process(new MyKeyProcess)

    stream.print()
    env.execute("UpdateTemp")
  }

  //核心的处理逻辑
  class MyKeyProcess extends KeyedProcessFunction[String, SensorReading, String] {
    //使用懒加载，声明两个变量，懒加载只会加载一次
    //申明温度变量，这里不实用 scala的 var ，是因为 flink自带的状态变量会把状态保存到磁盘或者hdfs上，可以恢复过来
    lazy val temptureValueState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-tempture", Types.of[Double]))
    lazy val timestampValueState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("last-time", Types.of[Long]))

    //每次来一个元素都要进行比较
    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
      //获取温度， 更新温度
      val preTemp = temptureValueState.value()
      temptureValueState.update(value.temperature);
      println(value)
      //获取上一次的定时逻辑
      val lastTimer = timestampValueState.value();
      //比较逻辑，如果是第一次 或者出现上次温度比这次低的话，就清楚定时器
      if (preTemp == 0.0 || preTemp >= value.temperature) {
        //首次清楚定时器，并且删掉注册逻辑
        ctx.timerService().deleteProcessingTimeTimer(lastTimer)
        timestampValueState.clear()
      } else if (lastTimer == 0L && preTemp < value.temperature) {
        //如果是第一次，并且上一次温度比这一次的要低的话，需要更新触发器,注意这里的定时器设置为5s
        val currectTime = ctx.timerService().currentProcessingTime() + 3000L
        ctx.timerService().registerProcessingTimeTimer(currectTime)
        //并且保存时间状态
        timestampValueState.update(currectTime)
      }
    }

    // onTimer 定时器触发 回掉方法
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect(s"""检测到${ctx.getCurrentKey}连续3s内，温度上升!!!""")
      timestampValueState.clear()
    }
  }

}
