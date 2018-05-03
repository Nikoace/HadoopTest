package prod;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class brorrowBook {
    public static final Map<String,String> typename = new HashMap <String, String>(){{
            put ( "A", "马克思主义、列宁主义、毛泽东思想、邓小平理论" );
            put ( "B", "哲学、宗教" );
            put ( "C", "社会科学总论" );
            put ( "D", "政治、法律" );
            put ( "E", "军事" );
            put ( "F", "经济" );
            put ( "G", "文化、科学、教育、体育" );
            put ( "H", "语言、文字" );
            put ( "I", "文学" );
            put ( "J", "艺术" );
            put ( "K", "历史、地理" );
            put ( "N", "自然科学总论" );
            put ( "O", "数理科学和化学" );
            put ( "P", "天文学、地球科学" );
            put ( "Q", "生物科学" );
            put ( "R", "医药、卫生" );
            put ( "S", "农业科学" );
            put ( "T", "工业技术" );
            put ( "U", "交通运输" );
            put ( "V", "航空、航天" );
            put ( "X", "环境科学、安全科学" );
            put ( "Z", "综合性图书" );

        }};

    public static class map extends Mapper<Object, Text, Text, Text> {
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
                        && !bookType.equals ( "保" )  && !bookType.equals ( "" )) {
                    context.write ( new Text ( bookType ), new Text ( "0" ) );
                }
            }
        }
    }

    public static class reduce extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable <Text> values, Context output) throws IOException, InterruptedException {
            Integer sum=0;
            for (Text val:values){
                sum = sum+1;
            }
            output.write ( new Text ( key+"\t"+typename.get ( key.toString () ) ),new Text ( sum.toString () ) );
        }
    }
}
