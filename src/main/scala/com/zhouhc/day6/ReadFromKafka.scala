package com.zhouhc.day6

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._



//从kafka中读取数据，只是简单的打印数据
object ReadFromKafka {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //kafka source 的配置，其他方式已经被废弃了
    val kafkaSource = KafkaSource.builder[String]().setBootstrapServers("zhc1:9092").setTopics("myTopic").setGroupId("myTest")
      .setStartingOffsets(OffsetsInitializer.latest()).setValueOnlyDeserializer(new SimpleStringSchema()).build()

    env.fromSource(kafkaSource,WatermarkStrategy.noWatermarks(),"Kafka Source").print()
    env.execute("ReadFromKafka");
  }
}
