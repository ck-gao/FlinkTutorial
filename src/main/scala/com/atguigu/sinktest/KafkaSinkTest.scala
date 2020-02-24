package com.atguigu.sinktest

import java.util.Properties

import com.atguigu.streamapitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object KafkaSinkTest {


  def main(args: Array[String]): Unit = {
    //0. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //全局设置并行度1，
    env.setParallelism(1)
    val stream2 = env.readTextFile("D:\\workspace\\IdeaProject\\FlinkTutorial\\src\\main\\resources\\sensor.txt").setParallelism(1)

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
    //addSource()通用方法

    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    //Transform操作
    val dataStream: DataStream[String] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString
      })

    dataStream.addSink(new FlinkKafkaProducer011[String]("hadoop102:9092", "sink", new SimpleStringSchema()))

    env.execute("kafka sink test")
  }
}










