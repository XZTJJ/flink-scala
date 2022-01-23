package com.zhouhc.day7

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

//使用CEP进行 检测订单的超时事件，
object CEPForDetectOrder {

  case class OrderCase(orderId: String, orderType: String, orderTime: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //添加数据源 ,赋予时间戳，分流
    val stream = env.fromElements(OrderCase("order_1", "create", 1000L), OrderCase("order_2", "create", 2000L), OrderCase("order_2", "pay", 6000L), OrderCase("order_1", "pay", 8000L))
      .assignAscendingTimestamps(_.orderTime).keyBy(_.orderId)

    //定义Pattern
    val pattern = Pattern.begin[OrderCase]("create").where(_.orderType.equalsIgnoreCase("create"))
      .next("pay").where(_.orderType.equalsIgnoreCase("pay"))
      .within(Time.seconds(5))

    //定义一个侧边输出标签
    val outsideTag = new OutputTag[String]("timeout")
    //正常输出的订单
    val selectFunc = (map: scala.collection.Map[String, Iterable[OrderCase]], out: Collector[String]) => {
      val create = map("create").iterator.next()
      out.collect(s"""正常完成的订单为 ${create.orderId}""")
    }

    val timeout = (map: scala.collection.Map[String, Iterable[OrderCase]], ts: Long, out: Collector[String]) => {
      val create = map("create").iterator.next()
      out.collect(s"""没有完成的订单为 ${create.orderId}， 超时时间为${ts}""")
    }
    //正常输出数据,接受三个处理函数，侧边输出标签，超时时间，正常输出时间
    val selectStream = CEP.pattern(stream, pattern).flatSelect(outsideTag)(timeout)(selectFunc)
    selectStream.print()
    selectStream.getSideOutput(outsideTag).print()

    env.execute("CEPForDetectOrder")
  }
}
