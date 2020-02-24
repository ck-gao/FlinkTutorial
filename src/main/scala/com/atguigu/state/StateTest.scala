package com.atguigu.state

import java.util.concurrent.TimeUnit

import com.atguigu.processfunction.TempIncreWarning
import com.atguigu.streamapitest.SensorReading
import org.apache.flink.api.common.functions.{RichFilterFunction, RichFlatMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object StateTest {
  def main(args: Array[String]): Unit = {
    //0. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //全局设置并行度1，
    env.setParallelism(1)
    //配置状态后端，引入包支持RocksDBStateBackend,保存状态，保证容错机制;
    //env.setStateBackend(new RocksDBStateBackend(""))
    //val stream2 = env.readTextFile("D:\\workspace\\IdeaProject\\FlinkTutorial\\src\\main\\resources\\sensor.txt").setParallelism(1)

    //checkpoint设置
    env.enableCheckpointing(10000L)//使能检查点，且传入两个分界线检查点的间隔，多长时间触发一次checkpoint barrir
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)//默认是精确一次性模式，AT_LEAST_ONCE：至少一次
    env.getCheckpointConfig.setCheckpointTimeout(60000L)//做checkpoint多长时间算超时时间
    env.getCheckpointConfig.setFailOnCheckpointingErrors(true)//做checkpoint过程时报错，true会挂掉整个任务
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)//最多有几个checkpoint同时进行(多个checkpoint在异步情况下发生)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)//最小间隔时间，第一个checkpoint结束点到下一个checkpoint头的间隔,如果设置那么MaxConcurrent失效
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)//job结束是否保留checkpoint，开启持久化保存checkpoint

    //重启策略(checkpoint开启后，怎么恢复？从最近一次成功保存的checkpoint状态自动恢复，如果恢复失败了，需要重启吗？)
    //1. 固定间隔时间重启策略  参数：尝试重启次数,尝试重启的间隔时间
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10000L))
    //2. 失败率重启策略，参数：(失败率，失败时间间隔5分钟，延迟时间间隔)
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3,Time.of(5,TimeUnit.MINUTES),Time.of(10,TimeUnit.SECONDS)))
    //fallBackRestart() //3. 回滚策略
    env.setRestartStrategy(RestartStrategies.noRestart())//4. 不回滚

    val inputStream = env.socketTextStream("hadoop102", 7777)

    //Transform操作
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })

    val processStream = dataStream
      .keyBy(_.id)
      //状态编程方式：flink提供用状态编程操作flatMap的操作
      .flatMap(new TempChangeWarning(10.0))

    val processStream2 = dataStream
      .keyBy(_.id)
      //状态编程方式：两个参数：R:输出的数据类型;  S保存状态的类型;
      .flatMapWithState[(String, Double, Double), Double]({
        //模式匹配
        case (inputData: SensorReading, None) => (List.empty, Some(inputData.temperature))
        case (inputData: SensorReading, lastTemp: Some[Double]) => {
          //计算两次温度的差值
          val diff = (inputData.temperature - lastTemp.get).abs
          //判断差值是否大于给定的阈值
          if (diff > 10.0) {
            (List((inputData.id, lastTemp.get, inputData.temperature)), Some(inputData.temperature))
          } else {
            (List.empty, Some(inputData.temperature))
          }
        }
      })

    processStream2.print()
    env.execute("state test")
  }
}

//自定义 Rich FlatMap Function
class TempChangeWarning(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
  //定义一个状态，用于保存上一次的状态值
  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    //getRuntimeContext 运行时上下文RunTimeContext; 可以传递默认值;
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
  }

  override def flatMap(value: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
    //获取到上次的温度值
    val lastTemp = lastTempState.value()
    //计算两次温度的差值
    val diff = (value.temperature - lastTemp).abs
    //判断差值是否大于给定的阈值
    if (diff > threshold) {
      //直接主流输出三元组
      collector.collect((value.id, lastTemp, value.temperature))
    }
    //来了新数据需要更新状态
    lastTempState.update(value.temperature)
  }
}
