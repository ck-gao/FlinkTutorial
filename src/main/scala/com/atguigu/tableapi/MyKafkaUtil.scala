package com.atguigu.tableapi

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object MyKafkaUtil {
  // 真正的流处理,从kafka中读取数据
  // 先创建kafka相关配置
  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
  //消费者组id
  properties.setProperty("group.id", "consumer-group")
  //key value序列化
  properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  properties.setProperty("auto.offset.reset", "latest")

  def getConsumer(str: String): FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String](str, new SimpleStringSchema(), properties)
}
