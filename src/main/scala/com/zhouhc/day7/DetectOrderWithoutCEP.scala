package com.zhouhc.day7

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

//使用非 CEP 的方式 检测订单是否超时 ！？
object DetectOrderWithoutCEP {
  //订单事件
  case class OrderCase(orderId: String, orderType: String, orderTime: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //添加数据源 ,赋予时间戳，分流
    val stream = env.fromElements(OrderCase("order_1", "create", 1000L), OrderCase("order_2", "create", 2000L), OrderCase("order_2", "pay", 4000L))
      .assignAscendingTimestamps(_.orderTime).keyBy(_.orderId).process(new MyKeyProcess)

    stream.print()

    env.execute("DetectOrderWithoutCEP")
  }


  //使用底层api的方式打印输出流
  class MyKeyProcess extends KeyedProcessFunction[String, OrderCase, String] {
    //每一个key都需要保存一个状态
    var valueStatus: ValueState[OrderCase] = _

    //初始化状态变量
    override def open(parameters: Configuration): Unit = {
      valueStatus = getRuntimeContext.getState(new ValueStateDescriptor[OrderCase]("orderTime", Types.of[OrderCase]))
    }

    //处理每一个元素
    override def processElement(value: OrderCase, ctx: KeyedProcessFunction[String, OrderCase, String]#Context, out: Collector[String]): Unit = {
      //保存新订单
      if (value.orderType.equalsIgnoreCase("create")) {
        if (valueStatus.value() == null) {
          valueStatus.update(value)
          ctx.timerService().registerEventTimeTimer(value.orderTime + 5000L)
        }
      } else {
        //证明时pay事件，直接保存
        out.collect(s"""${value.orderId}正常被支付了""")
        valueStatus.update(value)
      }
    }

    //清除定时器
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, OrderCase, String]#OnTimerContext, out: Collector[String]): Unit = {
      if (valueStatus.value() != null && valueStatus.value().orderType.equalsIgnoreCase("create")) {
        out.collect(s"""${valueStatus.value().orderId}延迟了""")
      }
      valueStatus.clear()
    }

  }
}
