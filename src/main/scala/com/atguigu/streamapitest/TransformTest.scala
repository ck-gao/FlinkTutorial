package com.atguigu.streamapitest

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //流处理API DataStream API;  批处理API DataSet API;
    val streamFromFile: DataStream[String] = env.readTextFile("D:\\workspace\\IdeaProject\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    //Transform操作
    val dataStream: DataStream[SensorReading] = streamFromFile
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })
    dataStream

    //1. 滚动聚合，对不同id的温度值进行累加
    val sumStream: DataStream[SensorReading] = dataStream
      .keyBy(0) //keyBy之后是keyedStream类型
      .sum(2) //sum/reduce之后又是DataStream类型

    //(reduce) 对不同id的温度值，输出当前的温度+10,并将时间戳在聚合结果上+1
    val reduceStream = dataStream
      .keyBy("id")
      .reduce((reduceRes, curData) => SensorReading(reduceRes.id, reduceRes.timestamp + 1, curData.temperature + 10.0))

    //2. 分流操作，split之后变成SplitStream类型
    val splitStream: SplitStream[SensorReading] = dataStream
      .split(sensorData => {
        if (sensorData.temperature > 30)
          Seq("high")
        else
          Seq("low")
      })
    //select之后才真正分流，但类型又回到DataStream
    val highTempStream = splitStream.select("high")
    val lowTempStream = splitStream.select("low")
    val allTempStream = splitStream.select("high", "low") //同时将多个标签选择出来

    //3. 合流操作
    val warningStream: DataStream[(String, Double, String)] = highTempStream
      .map(data => (data.id, data.temperature, "high temperature warning"))
    //ConnectedStreams[T,T2] T:三元组类型; T2是connect参数的类型
    val connectedStreams: ConnectedStreams[(String, Double, String), SensorReading] = warningStream
      .connect(lowTempStream)
    //map需要传递两个函数实现
    val coMapStream = connectedStreams.map(
      warningData=>(warningData._1,warningData._2),
      lowData=> (lowData.id,lowData.temperature,"healthy")
    )

    //union操作，要求类型必须一致，可以连续union
    val unionStream = highTempStream.union(lowTempStream)

    //highTempStream.print("high")
    //lowTempStream.print("low")
    //allTempStream.print("all")

    //4. 函数类实例,自定义
    val myFilterStream: DataStream[SensorReading] = dataStream.filter(new MyFilter())

    myFilterStream.print()
    env.execute("Transform test")
  }
}

//4. 自定义Filter Function
class MyFilter() extends FilterFunction[SensorReading]{
  override def filter(t: SensorReading): Boolean = {
      t.id.startsWith("sensor_1") //id以sensor_1开头
  }
}














