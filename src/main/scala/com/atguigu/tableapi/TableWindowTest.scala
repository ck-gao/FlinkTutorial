package com.atguigu.tableapi

import com.atguigu.streamapitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Table, TableEnvironment}

import org.apache.flink.table.api.scala._


object TableWindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置时间语义  EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputStream = env.readTextFile("D:\\workspace\\IdeaProject\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    val dataStream = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim,
          dataArray(1).trim.toLong,
          dataArray(2).trim.toDouble)
      })
      //分配watermark，设置watermark延迟时间
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        //提起时间戳
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })

    //基于env创建 tableEnv
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    //从一条流创建一张表，按照字段去定义，并指定事件时间的时间字段 => ts.rowtime   (处理时间proctime)
    val dataTable: Table = tableEnv.fromDataStream(dataStream,'id,'temperature,'ts.rowtime)

    //按照时间开窗聚合统计
    val resultTable: Table = dataTable
      .window(Tumble over 10.seconds on 'ts as 'tw) //Tumble 滚动窗口 over 10.seconds 10秒滚动一次 on 时间字段 as 别名
      .groupBy('id, 'tw) //分组聚合，如果groupBy的话，需要加入窗口别名，因为是基于窗口groupBy的
      .select('id, 'id.count) //选择对应字段

    val resultSqlTable: Table = tableEnv.sqlQuery("select id,count(id) from "
      + dataTable + " group by id, tumble(ts,interval '15' second)")

    //Boolean表示数据是老数据还是新数据
    val selectedStream: DataStream[(Boolean, (String, Long))] = resultTable
      .toRetractStream[(String, Long)] // groupBy之后不能使用toAppendStream

    selectedStream.print()

    env.execute("table window test")
  }
}
