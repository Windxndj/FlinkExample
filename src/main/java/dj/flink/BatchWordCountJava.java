package dj.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author: Dj
 * @Description:
 * @date 2020年02月09日 22:32:26
 */
public class BatchWordCountJava {
    public static void main(String[] args) throws Exception {
        String fileInputPath = "D:\\data";
        String fileOutputPath = "D:\\data";

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        获取文件内容
        DataSource<String> text = env.readTextFile(fileInputPath);

//        数据处理第二种方法(AggregateOperator/DataSet都可)
        AggregateOperator<Tuple2<String, Integer>> results = text.flatMap(new Tokenizer())
                .groupBy(0)
                .sum(1);

//        指定输出位置
        //输出到控制台
        results.print();
        //输出到文件
//        results.writeAsCsv(fileOutputPath, "\n", "\t")
                //设置并行度(把多个小文件变成1个文件显示)
//                .setParallelism(1);


        env.execute("batch word count ");

    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] splits = value.toLowerCase().split("\\W+");
            for (String split : splits) {
                if (split.length() > 0) {
                    out.collect(new Tuple2<>(split, 1));
                }
            }
        }
    }
}
