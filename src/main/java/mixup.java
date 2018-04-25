import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Vector;

public class mixup {
    public static class map extends Mapper<Object, Text, Text, Text> {
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit ();
            String path = fileSplit.getPath ().toString ();
            String line = value.toString ();
            if (line == null || line.equals ( "" )){
                return;
            }
            if (path.contains ( "avg" )){
                String[] val = line.split ( "\t" );
                if (val.length < 3){
                    return;
                }
                String id = val[0];
                String year = val[1];
                String grade = val[2];
                context.write ( new Text ( id + "\t"+ year),new Text ( "a#"+"\t"+grade ) );
            }else if (path.contains ( "door" )){
                String[] val = line.split ( "\t" );
                if (val.length < 3){
                    return;
                }
                String id = val[0];
                String count_9 = val[1];
                String count_11 = val[2];
                String year = "null";
                context.write ( new Text(id + "\t"+ year), new Text ( "b#" + count_9+count_11 ) );
            }else if (path.contains ( "lib" )){
                String[] val = line.split ( "\t" );
                if (val.length < 5){
                    return;
                }
                String name = val[0];
                String id = val[1];
                String year = val[2];
                String num = val[3];
                String type = val[4];
                context.write ( new Text(id + "\t"+ year ), new Text ( "c#" + name + num + type ) );
            }else if (path.contains ( "ykt" )){
                String[] val = line.split ( "\t" );
                if (val.length < 3){
                    return;
                }
                String id = val[0];
                String year = val[1];
                String custom = val[2];
                context.write ( new Text(id + "\t"+ year ), new Text ( "d#" + custom ) );
            }
        }
    }
    public static class reduce extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException {
            Vector<String> vA = new Vector <String> (  );
            Vector<String> vB = new Vector <String> (  );
            Vector<String> vC = new Vector <String> (  );
            Vector<String> vD = new Vector <String> (  );
            int count = 0;
            for (Text val:values){
                if (val.toString ().startsWith ( "a#" )){
                    vA.add ( val.toString ().substring ( 2 ) );
                }else if (val.toString ().startsWith ( "b#" )){
                    vB.add (val.toString ().substring ( 2 ));
                }else if (val.toString ().startsWith ( "c#" )){
                    vC.add (val.toString ().substring ( 2 ));
                }else if (val.toString ().startsWith ( "d#" )){
                    vD.add (val.toString ().substring ( 2 ));
                }
            }

            int sizeA = vA.size ();
            int sizeB = vB.size ();
            int sizeC = vC.size ();
            int sizeD = vD.size ();


            for (int i = 0; i<sizeA;i++){
                for (int j = 0; j<sizeB;j++){
                    for (int k = 0; j<sizeC;j++){
                        for (int l = 0; l<sizeD;j++){
                            context.write ( new Text ( key ),new Text (  vA.get ( i )+vB.get ( j )+vC.get ( i )+vD.get ( j ) ) );

                        }
                    }
                }
            }

        }
    }
}
