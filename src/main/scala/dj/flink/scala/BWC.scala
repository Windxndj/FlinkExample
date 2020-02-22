package dj.flink.scala

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @author: Dj
  * @Description:
  * @date 2020年02月22日 12:19:40
  *
  */
object BWC {

  def main(args: Array[String]): Unit = {
    var filePath: String = "E:///"
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.readTextFile(filePath)
    import org.apache.flink.api.scala.createTypeInformation
    val res = data.flatMap(_.split("\t").map((_, 1))).groupBy(0).sum(1)
    res.print()
  }


}
