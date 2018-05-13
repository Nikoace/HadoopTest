package prac;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class booksuit {
    public static class map extends Mapper <Object, Text, Text, Text> {
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString ();
            int relation;
            String[] val = line.split ( "\t" );
            if (val.length < 3) {
                return;
            }
            String id = val[0];
            String name = val[1];
            String list = val[2];

            if (!list.startsWith ( "-" )) {
                context.write ( new Text ( id + "\t" + name ), new Text ( list ) );
            }
        }
    }

    public static class reduce extends Reducer <Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable <Text> values, Context output) throws IOException, InterruptedException {
            StringBuilder rentbook = new StringBuilder ();
            for (Text val : values) {
                rentbook.append ( val.toString ()+"\t" );
            }
            output.write ( key,new Text ( rentbook.toString () ) );
        }
    }
}
