import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Vector;

public class book {
    public static class map extends Mapper<Object, Text, Text, Text> {
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit ();
            String path = fileSplit.getPath ().toString ();
            String line = value.toString ();
            if (line == null || line.equals ( "" )){
                return;
            }
            if (path.contains ( "lib.txt" )){
                String[] val = line.split ( "," );
                if (val.length < 5){
                    return;
                }
                String name = val[0];
                String id = val[1];
                String type = val[2];
                String bookId = val[3];
                String time = val[4];
                if (type.equals ( "借书" )) {
                    context.write ( new Text ( bookId ), new Text ("a#" + name + "\t" + id + "\t" + time.substring ( 0, 4 ) ) );
                }
            }else if (path.contains ( "bookList.txt" )){
                String[] val = line.split ( "," );
                if (val.length < 4){
                    return;
                }
                String booktype = val[0];
                String bookListId = val[1];

                context.write ( new Text(bookListId), new Text ( "b#" + booktype ) );
            }
        }
    }
    public static class reduce extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException {
            Vector<String> vA = new Vector <String> (  );
            Vector<String> vB = new Vector <String> (  );
            int count = 0;
            for (Text val:values){
                if (val.toString ().startsWith ( "a#" )){
                    vA.add ( val.toString ().substring ( 2 ) );
                }else if (val.toString ().startsWith ( "b#" )){
                    vB.add (val.toString ().substring ( 2 ));
                }
            }

            int sizeA = vA.size ();
            int sizeB = vB.size ();

            for (int i = 0; i<sizeA;i++){
                for (int j = 0; j<sizeB;j++){
                    context.write ( new Text ( vA.get ( i ) ),new Text (  vB.get ( j ) ) );
                }
            }

        }
    }
}
