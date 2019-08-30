package com.corp.flink.scala

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WordCountScala {
  def main(args: Array[String]): Unit = {
    // 先得到执行环境，我们这里选择流式环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    // 使用socket方式从我们的系统中获取数据
    val source = environment.socketTextStream("hadoop101" , 9999)

    val counts = source.flatMap(_.toLowerCase.split(" ")).filter(_.nonEmpty)
      .map {
        (_, 1)
      }
      .keyBy(0)
      .timeWindow(Time.seconds(10))
      .sum(1)
    counts.print()

    environment.execute("WordCountScala")
  }
}
