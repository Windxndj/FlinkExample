package dj.flink.scala

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @author: Dj
  * @Description: 基于java1.5(project structure->modle->language level:5) WC
  * @date 2020年01月31日 19:04:46
  *
  */
object SocketWindowWordCountScala {
  def main(args: Array[String]): Unit = {
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => {
        System.err.println("No port set.use default port 9000--scala")
      }
        9000
    }

    //获取运行环境 (:StreamExecutionEnvironment为返回类型数据)
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //链接socket获取输入数据
    val text = env.socketTextStream("windxn01", port, '\n')


    //注意：必须要添加这一行隐式转行,否则下面的flatmap方法执行会报错
    import org.apache.flink.api.scala._
    //解析数据(把数据打平),分组,窗口计算,并且聚合求sum
    val windowCounts = text.flatMap(line => line.split("\\s")) //打平,把每一行单词都切开
      .map(w => WordWithCount(w, 1)) //把单词转成word,1这种形式
      .keyBy("word") //分组
      .timeWindow(Time.seconds(2), Time.seconds(1)) //指定窗口大小,指定间隔时间
      .sum("count"); //sum或者reduce都可以
    //.reduce((a,b)=>WordWithCount(a.word,a.count+b.count))


    windowCounts.print().setParallelism(1)//对处理结果进行打印, 设置并行度(把执行结果放在哪)

//执行任务
    env.execute("Socket window count");

 }
//  通过内部类(隐式类)自动生成word,count和get/set方法来使用
  case class WordWithCount(word: String, count: Long)

}
