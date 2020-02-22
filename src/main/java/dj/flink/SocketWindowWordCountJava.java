package dj.flink;

import akka.stream.impl.fusing.Reduce;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author: Dj
 * @Description: 基于java1.8(project structure->modle->language level:8) WC
 * @date 2020年02月01日 18:10:34
 */
public class SocketWindowWordCountJava {
    public static void main(String[] args) throws Exception {
//定义socket连接的初始化参数
        int port;
        String hostname = "windxn01";
        String delimiter = "\n";

//运用异常处理操作try/catch设置控制台传默认参数
        try {
//            从传参中获取端口号
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
//            "port"传参数的key名称
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("No port set. Use default port 9000 --java");
            port = 9000;
        }
/**
 *@return: 1.*****     flink环境     *****
 **/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

/**
 *@return: 2. 获取socket里传输的数据(获取的内容)
 **/
        DataStreamSource<String> dataStreamSource = env.socketTextStream(hostname, port, delimiter);

/**
 *@Param: String输入的数据类型,
 *        WordWithCount返回的数据类型
 *@return: 3. 数据的处理****
 **/
        //a b c

        // a   1
        // b   1
        // c   1
        SingleOutputStreamOperator<WordWithCount> result
                = dataStreamSource.flatMap(new FlatMapFunction<String, WordWithCount>() {
            //        需要实现flatmap方法(详情看FlatMapFunction)
            @Override
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {

                String[] splits = value.split("\\s+");
                WordWithCount wordWithCount = new WordWithCount();
                for (String split : splits) {
                    wordWithCount.setWord(split);
                    wordWithCount.setCount(1L);
                    out.collect(wordWithCount);
                }
            }
            //为了防止出现  a   1   a   1 的重复数据,用keyBy去重
        }).keyBy("word")
                //滑动窗口大小(2s)间隔时长(1s)
                .timeWindow(Time.seconds(2L), Time.seconds(1))
                //反正就是求和怎么方便怎么来,sum/reduce都可
                .sum("count");
                /*
                *@Description:求和方法2:Reduce操作方法的操作,输入参数代表:相同的word的WordWithCount对象
                **/
        //   .reduce(new ReduceFunction<WordWithCount>() {
        //       @Override
        //      public WordWithCount reduce(WordWithCount wordWithCount, WordWithCount t1) throws Exception {
        //           return new WordWithCount(wordWithCount.getWord(), wordWithCount.getCount() + t1.getCount());
        //       }
        //   });

        /*
        *@Description: 对处理结果进行打印,设置并行度(把执行结果放在哪)
        **/
        //对处理结果进行打印
        result.print()
                //设置并行度
                .setParallelism(1);
/**
 *@return: 4.****程序执行命令****
 *@Description: Flink程序是延迟计算的，只有最后调用execute()方法的时候才会真正触发执行程序
 **/
        env.execute("socket ");
    }
}
