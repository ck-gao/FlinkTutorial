package com.atguigu.sinktest

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.atguigu.streamapitest.{SensorReading, SensorSource}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object JdbcSinkTest {
  def main(args: Array[String]): Unit = {
    //0. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //全局设置并行度1，
    env.setParallelism(1)
    //val stream2 = env.readTextFile("D:\\workspace\\IdeaProject\\FlinkTutorial\\src\\main\\resources\\sensor.txt").setParallelism(1)
    /*
        //Transform操作
        val dataStream: DataStream[SensorReading] = stream2
          .map(data => {
            val dataArray = data.split(",")
            SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
          })*/
    //变换一下数据源
    val dataStream = env.addSource(new SensorSource())


    dataStream.addSink(new MySQLJDBCSink())
    env.execute("JDBC Sink Text")
  }
}

//自定义Sink Function   (富函数编程:包含一些声明周期的方法)
class MySQLJDBCSink() extends RichSinkFunction[SensorReading] {
  //定义一个sql连接,放在open生命周期中创建连接
  var conn: Connection = _
  // 定义预编译语句
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  //1. 创建连接和预编译器
  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "111111")
    insertStmt = conn.prepareStatement("insert into temperatures (sensor,temp) values(?,?)")
    updateStmt = conn.prepareStatement("update temperatures set temp = ? where sensor = ?")
  }

  //2. 每来一条数据要处理一下invoke
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    //直接执行更新语句，如果没有查到那么执行插入语句
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()
    if (updateStmt.getUpdateCount == 0) {
      //如果没有更新，说明没有查询到对应的id，那么执行插入
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }

  //3. 关闭连接
  override def close(): Unit = {
    conn.close()
  }
}






