import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

public class avg {
    public static int time = 0;
    public static class Map extends Mapper<Object, Text, Text, IntWritable>{
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//            StringTokenizer tokenizer = new StringTokenizer ( value.toString () );
//            String student = tokenizer.nextToken ();//student
//            String year = tokenizer.nextToken ();//year
//            String strGrade = tokenizer.nextToken ();//grade
//            int grade = Integer.parseInt ( strGrade );
//            if (student.compareTo ( "XH" ) != 0){
//                context.write ( new Text ( student + "+" + year ), new IntWritable ( grade ) );
//            }
            StringTokenizer tokenizer = new StringTokenizer ( value.toString () );
            while (tokenizer.hasMoreTokens ()){
                String id = tokenizer.nextToken ();
                String year = tokenizer.nextToken ();
                String score = tokenizer.nextToken ();
                if (score.compareTo ( "CJ" )!=0) {
                    int scoreInt = Integer.parseInt ( score );
                    context.write ( new Text ( id + "+" + year ), new IntWritable ( scoreInt ) );
                }
            }
        }
    }
    public static class reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        protected void reduce(Text key, Iterable<IntWritable> values, Context output) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            Iterator<IntWritable> itr = values.iterator ();
            while (itr.hasNext ()){
                sum += itr.next ().get ();
                count++;
            }
            int avg = (int)sum/count;
            output.write ( key,new IntWritable ( avg ) );
        }
    }
}
