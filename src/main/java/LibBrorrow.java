import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class LibBrorrow {
    public static class map extends Mapper<Object,Text,Text,Text>{
        protected void map(Object key,Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer ( value.toString (),"\t");
            while (tokenizer.hasMoreTokens ()){
                String name = tokenizer.nextToken ();
                String id = tokenizer.nextToken ();
                String type = tokenizer.nextToken ();
                String bookId = tokenizer.nextToken ();
                String time = tokenizer.nextToken ();
                context.write ( new Text ( name + "\t" + id ),new Text ( type ) );
            }


        }
    }
    public static class reduce extends Reducer<Text,Text,Text,Text>{
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            Iterator<Text> itr = values.iterator ();
            while (itr.hasNext ()){
                String type = itr.next ().toString ();
                if (type.equals ( "ΩË È" )){
                    count += 1;
                }
            }
            String countString = String.valueOf ( count );
            context.write ( key,new Text ( countString ) );
        }
    }
}
