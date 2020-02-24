package com.atguigu.sinktest

import java.util

import com.atguigu.streamapitest.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object EsSinkTest {
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

    //es 集群
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop102", 9200))

    val esSearchSinkFuntion = new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        //每条数据来了怎么往es写，t：数据   runtimeContext上下文，requestIndexer：
        //包装好要发送到es的数据,必须要hashMap 或者  JsonObject格式
        val dataSource = new util.HashMap[String, String]()
        dataSource.put("sensor_id", t.id)
        dataSource.put("temperature", t.temperature.toString)
        dataSource.put("ts", t.timestamp.toString)
        //创建一个index request
        val indexReq = Requests.indexRequest().index("sensor").`type`("_doc").source(dataSource)
        //用 requestIndexer发送请求
        requestIndexer.add(indexReq)
      }
    }

    val esSinkBuilder: ElasticsearchSink.Builder[SensorReading] = new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
      esSearchSinkFuntion
    )
    esSinkBuilder

    //修改批次提交的数据数量(即来一条写一条)
    esSinkBuilder.setBulkFlushMaxActions(1)
    //使用esSinkBuilder.build()返回es的sink
    dataStream.addSink(esSinkBuilder.build())

    env.execute("Redis sink test")
  }
}







