package com.atguigu.sideoutput

import com.atguigu.processfunction.TempIncreWarning
import com.atguigu.streamapitest.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputTest {
  def main(args: Array[String]): Unit = {
    //0. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //全局设置并行度1，
    env.setParallelism(1)
    val stream2 = env.readTextFile("D:\\workspace\\IdeaProject\\FlinkTutorial\\src\\main\\resources\\sensor.txt").setParallelism(1)

    //val inputStream = env.socketTextStream("hadoop102", 7777)
    //Transform操作
    val dataStream: DataStream[SensorReading] = stream2
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })

    val highTempStream = dataStream
      .process(new SplitTempMonitor()) //不使用keyBy做process

    highTempStream.print("high")
    //获取侧输出流,打印测输出流中数据,low-temp为侧输出流id
    highTempStream.getSideOutput(new OutputTag[SensorReading]("low-temp")).print("low")

    env.execute("sideoutput test")
  }
}

//自定义process function 实现分流操作              [In,Out]        主流输出类型
class SplitTempMonitor() extends ProcessFunction[SensorReading, SensorReading] {
  //侧输出流输出类型在processElement指定
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    //ctx可以做时间服务
    //ctx.timerService()
    //侧输出流操作
    if (value.temperature < 30.0) {
      //输出数据到侧输出流，注意id要一致;
      ctx.output(new OutputTag[SensorReading]("low-temp"), value)
    } else {
      //30读以上的数据输出到主流
      out.collect(value)
    }
  }
}
