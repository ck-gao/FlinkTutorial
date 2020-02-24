package com.atguigu.tableapi

import com.atguigu.streamapitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._

object TableAPITest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //创建数据源
    val inputStream = env.readTextFile("D:\\workspace\\IdeaProject\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    val dataStream = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })

    //创建流式TableEnv (底层还是DataStream，在此基础上创建TableEnv)
    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

    //流转换成表  ==> 生成动态表    (按照样例类字段创建表;也可以自己指定字段定义)
    val dataTable: Table = tableEnv.fromDataStream(dataStream)

    // 从表里选取特定的数据
    val selectedTable: Table = dataTable.select('id, 'temperature) //'id使用这种方式，需要隐式转换import org.apache.flink.table.api.scala._
      .filter("id = 'sensor_1'")

    //table表转换成流DataStream[(String,Double)] ,select出来为二元组(id,temperature)
    val selectedStream: DataStream[(String, Double)] = selectedTable.toAppendStream[(String, Double)] //需要隐式转换才能使用toAppendStream

    //下面使用tableEnv.toAppendStream的方法不依赖隐式转换
    val selectedStream1: DataStream[(String, Double)] = tableEnv.toAppendStream[(String, Double)](selectedTable)

    selectedStream1.print()
    env.execute("Table API test")
  }
}
