import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Main {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        /*删除之前生成的OutPut文件夹*/

        FileSystem hdfs= FileSystem.get(conf);
        Path del = new Path ( "/avg" );
        boolean isDel = hdfs.delete ( del,true );

        /*验证hdfs路径（至少有两个）*/

        String[] otherArgs = new GenericOptionsParser (conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: avg <in> [<in>...] <out>");
            System.exit(2);
        }
        /*执行类*/
        Job job = Job.getInstance(conf, "avg");
        job.setJarByClass(avg.class);
        /*进行Mapreduce时使用*/
        job.setMapperClass(avg.Map.class);
        job.setReducerClass(avg.reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path (otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}