package prac;

import org.apache.commons.collections.map.MultiValueMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.mortbay.util.MultiMap;

import java.io.IOException;
import java.util.*;

public class percent {
    public static class map extends Mapper<Object,Text,Text,Text>{
        protected void map(Object key,Text value,Context context) throws IOException, InterruptedException {
            String line = value.toString ();
            int relation;
            String[] val = line.split ( "###" );
            if (val.length < 6){
                return;
            }
            String id = val[0];
            String name = val[1];
            String bookId = val[2];
            String bookName = val[3];
            String opType = val[4];
            String time = val[5];

            if (opType.equals ( "借书册数" )){
                context.write ( new Text ( bookName ),new Text ( id+"\t"+name ) );
            }
        }
    }
    public static class reduce extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable <Text> values, Context output) throws IOException, InterruptedException {
            String bookname = key.toString ();
            StringBuilder names = new StringBuilder (  );
            int nameNum = 0;
            ArrayList<String> nameList = new ArrayList <String> (  );
            for (Text val:values){
                String[] a= val.toString ().split ( "\t" );
                String name = a[1];
                nameList.add ( val.toString () );
                nameNum++;
                names.append ( name+"-" );
            }
            for (int i = 0; i < nameNum ; i++) {
                output.write ( new Text ( nameList.get ( i ) ),new Text ( bookname+"-"+names.toString () ));
            }
        }
    }
}
