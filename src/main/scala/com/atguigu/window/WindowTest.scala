package com.atguigu.window

import com.atguigu.sinktest.MySQLJDBCSink
import com.atguigu.streamapitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //全局设置并行度1，
    env.setParallelism(1)

    //设置时间语义，如果不设置默认是Processing Time时间语义;(参数为时间特性)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //env.getConfig.setAutoWatermarkInterval(100)//修改生成watermark的周期时间，默认200ms
    // (watermark对衡量事件时间有意义，watermark对processing event没有意义)

    //val stream2 = env.readTextFile("D:\\workspace\\IdeaProject\\FlinkTutorial\\src\\main\\resources\\sensor.txt").setParallelism(1)
    val inputStream = env.socketTextStream("hadoop102", 7777)
    //Transform操作
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })
      //1.1 对乱序数据分配时间戳和watermark(掌握); 有界的乱序(延迟发车)； 参数为最大延迟时间(延迟发车时间)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = {
          t.timestamp * 1000
        }
      })
    //.assignAscendingTimestamps(_.timestamp * 1000L) //1.2 提取时间戳(正常升序理想情况无乱序)，返回长整型类型作为时间戳（掌握）以timestamp为watermark
    // (flink中以毫秒处理)
    //new AssignerWithPeriodicWatermarks (1.3 先提取时间戳，然后基于时间戳周期性生成watermark,周期时间默认200ms,可以通过env设置)
    //new AssignerWithPunctuatedWatermarks(1.4 根据数据，打断点生成watermark)


    //统计每个床干起每15秒内的温度最小值
    val processedStream = dataStream
      .keyBy(_.id) //分组，可以传串样例类字段或下标//KeyedStream类型
      //.window(TumblingProcessingTimeWindows.of(Time.seconds(10))//WindowedStream类型
      //.window(Time.hours(1),Time.minutes(5))
      //.countWindow(10,2)//如果是计数窗口只能用countWindow,不能用window
      //.window(Time.hours(1),Time.minutes(5))//时间窗口可以用window或timeWindow
      //.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))//会话窗口底层是GlobalWindow,超过withGap时间就是一个新窗口
      //.GlobalWindows() //有头无尾，需要定义什么时候关，什么时候触发计算
      //.timeWindow(Time.seconds(15)) //定义长度为15秒的滚动窗口（窗口分配器）
      .timeWindow(Time.seconds(15), Time.seconds(3)) //滑动窗口中数据可能属于不同的窗口
      .allowedLateness(Time.seconds(5)) //暂时不关闭窗口，保持5秒再关闭;
      .sideOutputLateData(new OutputTag[SensorReading]("late")) //测输出流
      .reduce( //（窗口函数，具体数据操作）
        (curMinTempData, newData) =>
          SensorReading(
            curMinTempData.id,
            newData.timestamp,
            newData.temperature.min(curMinTempData.temperature))
      )

    processedStream.print()
    env.execute("Window Text")
  }
}
