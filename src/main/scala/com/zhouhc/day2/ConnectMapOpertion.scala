package com.zhouhc.day2

import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

//联合两条流，每个流中的元素可以不一样， 可以直接联合， 也可以通过 类似于sql的 on 的通过关键字联合的方式
object ConnectMapOpertion {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //动态获取两个不同的source
    val source1 = env.fromElements(("dapangzi", 180), ("xiaopangzi", 110), ("zhongpangzi", 140),("zhengchang", 90),("souzi",70))
    val source2 = env.fromElements(("dapangzi", "chaozhong"), ("xiaopangzi", "weipang"), ("zhongpangzi", "pianpang"),("zhengchang","jiankang"),("souzi","piansou"));

    //简单的直接联合,这样简单的联合没有什么意义
//        val simpleSource = source2.connect(source1)
//        simpleSource.flatMap(new MySimpleCoFlatMap).print()

    val conSource = source2.keyBy(_._1).connect(source1.keyBy(_._1))
    conSource.flatMap(new MyConFlatMap).print()

    env.execute("ConnectMapOpertion")
  }


  //简单的合并两条流，不做复杂的逻辑处理
  class MySimpleCoFlatMap extends CoFlatMapFunction[(String, String), (String, Int), String] {
    override def flatMap1(value: (String, String), out: Collector[String]): Unit = {
      out.collect(s"""source1 : name is ${value._1}, weight is ${value._2}""")
    }

    override def flatMap2(value: (String, Int), out: Collector[String]): Unit = {
      out.collect(s"""source2 : name is ${value._1}, metrics is ${value._2}""")
    }
  }


  //keyBy后面的逻辑处理
  class MyConFlatMap extends CoFlatMapFunction[(String, String), (String, Int), String] {
    override def flatMap1(value: (String, String), out: Collector[String]): Unit = {
      out.collect(s"""and metrics is ${value._2}""")
    }

    override def flatMap2(value: (String, Int), out: Collector[String]): Unit = {
      out.collect(s"""source name is ${value._1}, weight is ${value._2}""")
    }
  }


}
