package com.zhouhc.day2

import org.apache.flink.streaming.api.scala._

//简单的合并数据,只能合并相同元素的流，并且各个流的元素先后顺序进入 到合并流中
object UnionOpertion {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment;
    env.setParallelism(1)

    val source = env.addSource(new SensorSource)
    //获取三部分数据
    val part1 = source.filter(_.id.equals("Sensor_1"))
    val part2 = source.filter(_.id.equals("Sensor_2"))
    val part3 = source.filter(_.id.equals("Sensor_3"))

    part1.union(part2, part3).print()

    env.execute("UnionOpertion");
  }

}
