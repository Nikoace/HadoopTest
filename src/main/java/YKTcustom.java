import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class YKTcustom {
    public static class map extends Mapper<Object,Text,Text,Text>{
        protected void map(Object key ,Text value,Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer ( value.toString () );
            String time = tokenizer.nextToken ();//消费时间
            String cash = tokenizer.nextToken ();//余额
            String custom = tokenizer.nextToken ();//消费额/充值额
            String id = tokenizer.nextToken ();//学号
            String customType = tokenizer.nextToken ();//消费类型
            context.write ( new Text ( id + "+" + time + "+" + customType ),new Text ( custom ) );
        }
    }

    public static class reduce extends Reducer<Text,Text,Text,Text>{
        protected void reduce(Text key, Text values, Context context){
            int sum=0;

        }
    }
}
