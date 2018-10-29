import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;

public class UniqueURLJob extends Configured implements Tool {
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        static final IntWritable one = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] partsOfData = value.toString().split("\t");
            if (partsOfData.length != 3){
                return;
            }
            String userId = partsOfData[0];
            String dateTime = partsOfData[1];
            URL url;
            try {
                url = new URL(partsOfData[2]);
            }catch (Exception ex){
                System.out.println(partsOfData[2]);
                return;
            }
            context.write(new Text(partsOfData[2]), one);
        }
    }


    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        static final IntWritable one = new IntWritable(1);
        @Override
        protected void reduce(Text word, Iterable<IntWritable> nums, Context context) throws IOException, InterruptedException {
            context.write(word, one); // То, что в коде лекций emit
        }
    }


    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(UniqueURLJob.class);
        job.setJobName(UniqueURLJob.class.getCanonicalName());

        job.setNumReduceTasks(5);
        TextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new UniqueURLJob(), args);
        System.exit(ret);
    }
}
