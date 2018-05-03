package prac;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class ChildParents{
    public static int time = 0;
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
            // 左右表的标识
            int relation;
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            String child = tokenizer.nextToken();
            String parent = tokenizer.nextToken();
            if (child.compareTo("C") != 0) {
                // 左表
                relation = 1;
                context.write(new Text(parent),
                        new Text(relation + "+" + child));//parent(key)->1+child(value)
                // 右表
                relation = 2;
                context.write(new Text(child),
                        new Text(relation + "+" + parent));//child->2+parent
            }
        }

    }
    public static class reduce extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context output) throws java.io.IOException, InterruptedException {
            int grandchildnum = 0;
            int grandparentnum = 0;
            List<String> grandchilds = new ArrayList<String> ();
            List<String> grandparents = new ArrayList<String>();

            /* 输出表头 */
            if (time == 0) {
                output.write(new Text("grandchild"), new Text("grandparent"));
                time++;
            }
            while (values.iterator ().hasNext ()){
                Text val = values.iterator ().next ();
                String record = val.toString();
                char relation = record.charAt(0);
                // 取出此时key所对应的child
                if (relation == '1') {
                    String child = record.substring(2);
                    grandchilds.add(child);
                    grandchildnum++;
                }
                // 取出此时key所对应的parent
                else if (relation == '2'){
                    String parent = record.substring(2);
                    grandparents.add(parent);
                    grandparentnum++;
                }
            }
            if (grandchildnum != 0 && grandparentnum != 0) {
                for (int i = 0; i < grandchildnum; i++)
                    for (int j = 0; j < grandparentnum; j++)
                        output.write(new Text(grandchilds.get(i)), new Text(
                                grandparents.get(j)));
            }

        }
    }
}