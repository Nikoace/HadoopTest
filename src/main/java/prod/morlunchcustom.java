package prod;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

public class morlunchcustom {
    /*
    isEffectiveDate 定义时间段
    cusTime 消费时间
    startTime 时间段开始
    endTime 时间段结束
     */
    public static boolean isEffectiveDate(Date cusTime, Date startTime, Date endTime) {
        if (cusTime.getTime () == startTime.getTime () || cusTime.getTime () == endTime.getTime ()) {
            return true;
        }//如果消费时间刚好等于时间段的开头结尾
        Calendar date = Calendar.getInstance ();
        date.setTime ( cusTime );

        Calendar begin = Calendar.getInstance ();
        begin.setTime ( startTime );

        Calendar end = Calendar.getInstance ();
        end.setTime ( endTime );

        if (date.after ( begin ) && date.before ( end )) {
            return true;
        } else {
            return false;
        }//判断是否在时间段内
    }

    public static class map extends Mapper <Object, Text, Text, Text> {
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // 定义时间格式
            String format = "HH:mm:ss";

            // 分割value字符串
            StringTokenizer tokenizer = new StringTokenizer ( value.toString (), "," );

            String id = tokenizer.nextToken ().trim ();
            String name = tokenizer.nextToken ().trim ();
            String customCost = tokenizer.nextToken ();
            String cash = tokenizer.nextToken ();
            String customType = tokenizer.nextToken ().trim ();
            String customTime = tokenizer.nextToken ();
            String customEnv = tokenizer.nextToken ();

            //分割消费时间为日期和具体时间
            String[] all = customTime.split ( " " );
            String year = all[0];
            String time = all[1];
            try {
                //转化String类到Date
                Date cusTime = new SimpleDateFormat ( format ).parse ( time );
                Date startTime = new SimpleDateFormat ( format ).parse ( "21:00:59" );//定义开始时间
                Date endTime = new SimpleDateFormat ( format ).parse ( "24:00:59" );//定义结束时间

                if (isEffectiveDate ( cusTime, startTime, endTime ) && year.contains ( "2017" )) {
                    context.write ( new Text ( id + "\t" + name ), new Text ( customCost + "\t" + customEnv ) );
                }
            } catch (Exception e) {

            }
        }
    }

    public static class reduce extends Reducer <Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable <Text> values, Context output) throws IOException, InterruptedException {
            Double sum = 0.0;
            String env = "null";
            StringBuilder suki = new StringBuilder ();
            HashMap <String, Integer> envs = new HashMap <String, Integer> ();
            for (Text val : values) {

                String[] line = val.toString ().split ( "\t" );
                env = line[1].replaceAll ( "\\s*", "" );
                Double cost = Double.valueOf ( line[0] );
                if (!env.contains ( "四川大学锦城学院" ) && !env.contains ( "售卡室" ) && !env.contains ( "文印中心" ) && !env.contains ( "学生公司印务中心" ) && !env.contains ( "车队" )
                        && !env.contains ( "交行转入" ) && !env.contains ( "美来生活" ) && !env.contains ( "体育馆" ) && !env.contains ( "图书馆" )
                        && !env.contains ( "杏岛会所" ) && !env.contains ( "杏岛会所1" ) && !env.contains ( "医务室" ) && !env.contains ( "加油站" )) {
                    if (envs.containsKey ( env )) {
                        envs.put ( env, envs.get ( env ) + 1 );
                    } else {
                        envs.put ( env, 1 );
                    }
                    sum = sum + cost;
                }
            }

            List <Map.Entry <String, Integer>> list = new ArrayList <Map.Entry <String, Integer>> ( envs.entrySet () );
            Collections.sort ( list, new Comparator <Map.Entry <String, Integer>> () {
                @Override
                public int compare(Map.Entry <String, Integer> o1, Map.Entry <String, Integer> o2) {
                    return o2.getValue () - o1.getValue ();
                }
            } );

            int k = 0;
            for (Iterator iterator = list.iterator (); iterator.hasNext () && k < 2; ++k) {
                Map.Entry entry = (Map.Entry) iterator.next ();
                suki.append ( entry.getKey ().toString () + "/" );
            }
            String sukiString = suki.toString ();
            BigDecimal bigDecimal = new BigDecimal ( sum );
            Double sumBig = bigDecimal.setScale ( 2, BigDecimal.ROUND_HALF_UP ).doubleValue ();
            output.write ( key, new Text ( sumBig.toString () + "\t" + sukiString ) );
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration ();

        /*删除之前生成的OutPut文件夹*/

        FileSystem hdfs = FileSystem.get ( conf );
        Path del = new Path ( "/test1" );
        boolean isDel = hdfs.delete ( del, true );

        /*验证hdfs路径（至少有两个）*/

        String[] otherArgs = new GenericOptionsParser ( conf, args ).getRemainingArgs ();
        if (otherArgs.length < 2) {
            System.err.println ( "Usage: prac.avg <in> [<in>...] <out>" );
            System.exit ( 2 );
        }
        /*执行类*/
        Job job = Job.getInstance ( conf, "test" );
        job.setJarByClass ( morlunchcustom.class );
        /*进行Mapreduce时使用*/
        job.setMapperClass ( morlunchcustom.map.class );
        job.setReducerClass ( morlunchcustom.reduce.class );
        job.setOutputKeyClass ( Text.class );
        job.setOutputValueClass ( Text.class );

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath ( job, new Path ( otherArgs[i] ) );
        }
        FileOutputFormat.setOutputPath ( job,
                new Path ( otherArgs[otherArgs.length - 1] ) );
        System.exit ( job.waitForCompletion ( true ) ? 0 : 1 );
    }
}
