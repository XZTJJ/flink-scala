package com.zhouhc.day2

import org.apache.flink.streaming.api.scala._

//简单的分组数据，相同的Key分到同一个slot中
object MyKeyByOpertion {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val keyBy = env.addSource(new SensorSource).filter(ele => ele.id.equals("Sensor_1")).keyBy(_.id).min(2)
    keyBy.print();
    env.execute("MyKeyByOpertion");
  }

}
