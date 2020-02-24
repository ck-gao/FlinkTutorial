package com.atguigu.processfunction

import com.atguigu.streamapitest.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    //0. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //全局设置并行度1，
    env.setParallelism(1)
    //val stream2 = env.readTextFile("D:\\workspace\\IdeaProject\\FlinkTutorial\\src\\main\\resources\\sensor.txt").setParallelism(1)

    val inputStream = env.socketTextStream("hadoop102", 7777)

    //Transform操作
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })

    val processStream = dataStream
      //.process() //如果基于普通Stream那么里面直接传入ProcessFuntion
      //.keyBy("id") //如果传字符串，那么继承KeyedProcessFunction类型1要为Tuple
      //如果想使用String，那么可以写成.keyBy(_.id)
      .keyBy(_.id)
      .process(new TempIncreWarning()) //keyBy以后传入自定义function

    processStream.print()
    env.execute("processfunction test")
  }
}

//自定义实现process function                           //Tuple
class TempIncreWarning() extends KeyedProcessFunction[String, SensorReading, String] {
  //lastTempState需要在open时先创建出来，或者使用 lazy懒加载(什么时候用什么时候加载)
  //首先定义状态，用来保存上一次的温度值，以及已经设定的定时器时间戳;参数为描述其
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
  lazy val currentTimerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("current-time", classOf[Long]))

  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    //context.timerService().registerEventTimeTimer()  //注册一个事件时间定时器，闹钟事件到了，调用onTimer
    // 取出上次的温度值和定时器时间戳
    val lastTemp = lastTempState.value()
    val curTimerTs = currentTimerState.value()

    //将状态更新为最新的温度值
    lastTempState.update(i.temperature)

    //判断当前温度是否上升，如果上升而且没有注册定时器，那么注册10秒后的定时器
    if (i.temperature > lastTemp && curTimerTs == 0) { //curTimerTs为0表示没有注册定时器
      val ts = context.timerService().currentProcessingTime() + 5000L

      context.timerService().registerProcessingTimeTimer(ts)
      //保存时间戳到定时器状态
      currentTimerState.update(ts)
    } else if (i.temperature < lastTemp) {
      //如果下降，删除定时器和状态
      context.timerService().deleteProcessingTimeTimer(curTimerTs)
      currentTimerState.clear() //删除状态
    }
  }

  //只有keyedProcessFunction能使用onTimer
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    //    ctx.timeDomain() //时间域
    //Collector类型，向外部推出数据
    out.collect("sensor" + ctx.getCurrentKey + "温度在5秒内连续上升!")
    //定时器触发了之后，状态还在，需要清空
    currentTimerState.clear() //删除状态
  }
}
