package dj.flink.scala

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * @author: Dj
  * @Description:
  * @date 2020年02月10日 08:23:10
  *
  */
object BatchWordCountScala {


  def main(args: Array[String]): Unit = {
    val infile = ""
    val outfile = ""
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text =env.readTextFile(infile)
    //注意：必须要添加这一行隐式转行,否则下面的flatmap方法执行会报错
    import org.apache.flink.api.scala._
    val counts = text.flatMap(_.toLowerCase.split("\\W+")).filter(_.nonEmpty ).map((_,1)).groupBy(0).sum(1)
//    counts.print();
    counts.writeAsCsv(outfile,"\n","\t").setParallelism(1)
    env.execute("batch window count")
  }


}
