import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class LibBrorrow {


    public static class LibMap extends Mapper <Object, Text, Text, Text> {
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] val = value.toString ().split ( "\t" );
            if (val.length < 4) return;
            String name = val[0];
            String id = val[1];
            String year = val[2];
            String type = val[3];
            context.write ( new Text ( name + "\t" + id + "\t" + year ),new Text ( type )  );
        }
    }

    public static class reduce extends Reducer <Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            ArrayList<String> arrayList = new ArrayList <String> (  );
            StringBuffer add = new StringBuffer (  );
            for (Text val : values){
                count++;
                if (!arrayList.contains ( val.toString () )){
                    arrayList.add ( val.toString () );
                }

            }
            for (int i1=0;i1<arrayList.size ();i1++){
                add.append ( arrayList.get ( i1 ) + "/" );
            }
            String all = add.toString ();
            String countString = String.valueOf ( count );
            context.write ( key,new Text ( countString+"\t"+ all  ) );
        }
    }
}
