package com.atguigu.wc

//import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
// 批处理 word count
object WordCount {
  def main(args: Array[String]): Unit = {
    //1. 创建一个批处理的执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    env
    //从外部传入参数
    val params:ParameterTool = ParameterTool.fromArgs(args)
    val inputPath:String = params.get("input")
    //2. 从文件中批量读取数据
    //val inputPath = "D:\\workspace\\IdeaProject\\FlinkTutorial\\src\\main\\resources\\hello.txt"
    //批处理API : DataSet
    //流处理API : DataStream
    //DataSet[String]的每一个String是一行内容
    val inputDataSet:DataSet[String] = env.readTextFile(inputPath)

    //3. 对每一行的单词进行分词平铺展开，根据不同的word进行count聚合
    val wordCountDataSet:DataSet[(String,Int)] = inputDataSet
      .flatMap(_.split(" ")) //平铺展开
      .map((_,1)) //转换成二元组方便分组聚合
      .groupBy(0) //按照上面二元组下表为0的元素进行分组
      .sum(1) //按照二元组中序号为1的元素进行聚合

    //4. 打印输出
    wordCountDataSet.print()
  }
}
