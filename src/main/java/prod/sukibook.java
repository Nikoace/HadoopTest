package prod;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;


public class sukibook {
    public static int time = 0;

    public static class map extends Mapper <Object, Text, Text, Text> {
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString ();
            String[] val = line.split ( "\t" );
            Pattern p = Pattern.compile ( "[a-zA-Z]" );

            //
            if (val.length < 5) {
                return;
            }
            String id = val[0];
            String name = val[1];
            String bookType = val[2];
            String opType = val[3];
            String time = val[4];
            if (time.equals ( "2017" ) && opType.equals ( "借书册数" ) && !p.matcher ( id ).find ()) {
                if (bookType != null && !bookType.equals ( "/" ) && !bookType.equals ( "]" ) && !bookType.equals ( "]" )
                        && !bookType.equals ( "0" ) && !bookType.equals ( "2" ) && !bookType.equals ( "3" )
                        && !bookType.equals ( "保" )) {
                    context.write ( new Text ( id + "\t" + name ), new Text ( bookType ) );
                }
            }
        }
    }

    public static class reduce extends Reducer <Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable <Text> values, Context output) throws IOException, InterruptedException {
            //init
            String maxType = null;
            String maxnum = null;
            ArrayList <String> arrayList = new ArrayList <String> ();
            StringBuilder add = new StringBuilder ();
            Map <String, Integer> typeCount = new HashMap <String, Integer> ();

            if (time == 0) {
                output.write ( new Text ( "id\tname" ), new Text ( "favorite_book\tnumber\tborrow_kinds" ) );
                time++;
            }

            for (Text val : values) {
                String type = val.toString ();
                if (typeCount.containsKey ( type )) {
                    typeCount.put ( type, typeCount.get ( type ) + 1 );
                } else {
                    typeCount.put ( type, 1 );
                }

                if (!arrayList.contains ( val.toString () )) {
                    arrayList.add ( val.toString () );
                }
            }

            List <Entry <String, Integer>> list = new ArrayList <Entry <String, Integer>> ( typeCount.entrySet () );
            Collections.sort ( list, new Comparator <Entry <String, Integer>> () {
                @Override
                public int compare(Entry <String, Integer> o1, Entry <String, Integer> o2) {
                    return o2.getValue () - o1.getValue ();
                }
            } );

            int k = 0;
            for (Iterator iterator = list.iterator (); iterator.hasNext () && k < 1; ++k) {
                Entry entry = (Entry) iterator.next ();
                maxType = entry.getKey ().toString ();
                maxnum = entry.getValue ().toString ();
            }

            for (String anArrayList : arrayList) {
                add.append ( anArrayList ).append ( "/" );
            }
            String all = add.toString ();
            output.write ( key, new Text ( maxType + "\t" + maxnum + "\t" + all ) );
        }
    }
}

