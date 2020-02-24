package com.atguigu.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//隐式转换问题，需要引入下面的内容
import  org.apache.flink.api.scala._

//流处理 word count

object StreamWordCount {
  def main(args: Array[String]): Unit = {

    //从外部传入参数
    val params:ParameterTool = ParameterTool.fromArgs(args)
    val host:String = params.get("host")
    val port:Int = params.getInt("port")


    //1. 创建流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //全局指定并行度
    //env.setParallelism(8)

    //2. 从文本流 读取流式数据,(读数据源)
    //val textDataStream = env.socketTextStream("hadoop102",7777)
    val textDataStream = env.socketTextStream(host,port)

    //3. 进行转换处理，count
    val dataStream = textDataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty) //不能为空
      .map((_,1)) //映射成二元组("word",1)
      .keyBy(0)  //分组，需要指定key;以二元组中索引下标为0的元素作为key
      .sum(1) //可以在每个算子上设置不同的并行度

    //4. 打印输出
    dataStream.print().setParallelism(1)

    //启动任务,让流式环境执行起来
    env.execute()
  }
}
