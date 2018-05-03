package prac;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.StringTokenizer;

public class door {
    public static class map extends Mapper<Object,Text,Text,IntWritable>{
        protected void map(Object key,Text value,Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer ( value.toString () );
            while (tokenizer.hasMoreTokens ()){
                String id = tokenizer.nextToken ();
                String type = tokenizer.nextToken ();
                String timeString = tokenizer.nextToken ();
                int time = Integer.parseInt ( timeString );
                context.write ( new Text ( id  ), new IntWritable ( time ) );

            }
        }
    }

    public static class reduce extends Reducer<Text, IntWritable,Text,Text>{
        protected void reduce(Text key, Iterable<IntWritable> values, Context output) throws IOException, InterruptedException {
            int count_9 =0;
            int count_23 = 0;
            Iterator<IntWritable> itr = values.iterator ();
            while (itr.hasNext ()){
                int i = itr.next ().get ();
                if (i >= 210000 && i<=230000 ){
                    count_9+=1;
                }else if (i > 230000 || i <30000 ){
                    count_23+=1;
                }
            }
            String count = count_9 + "\t" + count_23;
            output.write ( key , new Text ( count ) );
        }
    }
}
