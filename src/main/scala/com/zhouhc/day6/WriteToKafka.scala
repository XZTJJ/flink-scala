package com.zhouhc.day6


import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}

import java.util.Properties


//数据写入kafka中
object WriteToKafka {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val source = env.fromElements("zhouhc" + System.currentTimeMillis, "zhouhc" + System.currentTimeMillis)

    //创建连接属性，使用EXACTLY_ONCE的话，必须设置 transaction.timeout.ms 属性，不然会报错
    val properties = new Properties
    properties.setProperty("transaction.timeout.ms", s"${60 * 5 * 1000}")
    //kafka的 sink配置 ，其他方式已经被废弃了
    val kafkaSink = KafkaSink.builder[String]().setBootstrapServers("zhc1:9092").setRecordSerializer(
      KafkaRecordSerializationSchema.builder().setTopic("myTopic").setValueSerializationSchema(new SimpleStringSchema()).build()).setKafkaProducerConfig(properties)
      .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE).build()

    //写入kafka中
    source.sinkTo(kafkaSink)
    env.execute("WriteToKafka")
  }
}
