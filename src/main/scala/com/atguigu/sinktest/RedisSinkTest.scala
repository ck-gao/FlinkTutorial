package com.atguigu.sinktest

import com.atguigu.streamapitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    //0. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //全局设置并行度1，
    env.setParallelism(1)

    val stream2 = env.readTextFile("D:\\workspace\\IdeaProject\\FlinkTutorial\\src\\main\\resources\\sensor.txt").setParallelism(1)
    //Transform操作
    val dataStream: DataStream[SensorReading] = stream2
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })

    //定义一个Flink jedis config
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("hadoop102")
      .setPort(6379)
      .build()

    dataStream.addSink(new RedisSink[SensorReading](conf,new MyRedisMapper))


    env.execute("Redis sink test")
  }
}

//自定义一个RedisMapper类
class MyRedisMapper extends  RedisMapper[SensorReading]{
  //用什么命令
  override def getCommandDescription: RedisCommandDescription = {
    //保存到redis的命令，存成哈希表 HSET  sensor_temp表明  key->id  value->temp
    new RedisCommandDescription(RedisCommand.HSET,"sensor_temp")
  }
  //key是什么
  override def getKeyFromData(t: SensorReading): String = t.temperature.toString
  //value是什么
  override def getValueFromData(t: SensorReading): String = t.id
}















