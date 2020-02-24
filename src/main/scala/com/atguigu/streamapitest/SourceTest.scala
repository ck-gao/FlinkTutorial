package com.atguigu.streamapitest

//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.util.{Properties, Random}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011


object SourceTest {
  def main(args: Array[String]): Unit = {
    //0. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //全局设置并行度1，
    env.setParallelism(1)
    //使OperatorChain失效，但是要求使并行度相同和one-to-one才有意义
    //env.disableOperatorChaining()

    //1. 从集合中读取数据,参数是集合
    val stream1 = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.72),
      SensorReading("sensor_10", 1547718205, 38.1)
    )
    )

    //各种类型都可以直接处理，测试时使用
    //env.fromElements(0,1.2,"true",false)

    //2. 从文件中读取数据
    //读取时设置并行度为1，保证读取数据和打印数据顺序一致(或全局env.setParallelism(1))
    val stream2 = env.readTextFile("D:\\workspace\\IdeaProject\\FlinkTutorial\\src\\main\\resources\\sensor.txt").setParallelism(1)

    //3. 真正的流处理,从kafka中读取数据
    //    env.socketTextStream("hadoop102",7777)
    //3.1 先创建kafka相关配置
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    //消费者组id
    properties.setProperty("group.id", "consumer-group")
    //key value序列化
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    //addSource()通用方法
    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    //4. 自定义输入源,addSource()通用方法
    val stream4 = env.addSource(new SensorSource())


    //打印输出
    stream3.print().setParallelism(1)
    env.execute("source test")
  }
}

//1. 定义样例类，传感器id 时间戳 温度(读集合)
case class SensorReading(id: String, timestamp: Long, temperature: Double)

//4. 自定义的source function类
class SensorSource() extends SourceFunction[SensorReading] {
  //核心run方法，不停的发出数据(通过sourceContext上下文发出数据到计算任务)
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    //定义一个随机数发生器
    val rand = new Random()
    //随机生成10个传感器的初始温度值，然后再它的基础上随机微变动
    var curTemp = 1.to(10) //1到10序列
      .map(
        //1到10的序列，转换成二元组(传感器id,温度值)
        i => ("sensor_" + i, 60 + rand.nextGaussian() * 20)
      )

    //如果没有被cancel，就无限循环产生数据
    while (running) {
      //在上次的温度值基础上，微小改变，生成当前新的温度值
      curTemp = curTemp.map(
        data => (data._1, data._2 + rand.nextGaussian())
      )
      //获取当前系统时间戳
      val curTime = System.currentTimeMillis()
      //把一组10个数据都添加上时间戳，全部输出
      curTemp.foreach(
        data => sourceContext.collect(SensorReading(data._1, curTime, data._2))
      )
      //时间间隔500毫秒
      Thread.sleep(500)
    }
  }

  //cancel取消发送数据，可以先设置标志位，用于控制是否发送数据
  var running: Boolean = true

  override def cancel(): Unit = {
    running = false
  }
}







