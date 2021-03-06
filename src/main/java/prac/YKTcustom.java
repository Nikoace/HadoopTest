package prac;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.StringTokenizer;

public class YKTcustom {
    public static class map extends Mapper<Object,Text,Text,DoubleWritable>{
        protected void map(Object key ,Text value,Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer ( value.toString () );
            String time = tokenizer.nextToken ();//消费时间
            String cash = tokenizer.nextToken ();//余额
            String custom = tokenizer.nextToken ();//消费额/充值额
            String id = tokenizer.nextToken ();//学号
            String customType = tokenizer.nextToken ();//消费类型
            String customTypeName = tokenizer.nextToken ();
            if (customType.equals ( "2032" )||customType.equals ( "2033" )||customType.equals ( "2042" )||customType.equals ( "2054" )||
                    customType.equals ( "2071" )||customType.equals ( "3032" )||customType.equals ( "3042" )||customType.equals ( "3054" )||
                    customType.equals ( "3071" )){
                if (!time.equals ( "NULL" )){
                    double customD = Double.parseDouble ( custom );
                    context.write ( new Text ( id + "\t" + time ), new DoubleWritable ( customD ) );
                }
            }
        }
    }

    public static class reduce extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            Double sum=0.0;
            Iterator<DoubleWritable> itr = values.iterator ();
            while (itr.hasNext ()){
                sum += itr.next ().get ();

            }
            BigDecimal b = new BigDecimal ( sum );
            Double sumBig = b.setScale ( 2, BigDecimal.ROUND_HALF_UP ).doubleValue ();
            context.write ( key, new DoubleWritable ( sumBig ) );
        }
    }
}
