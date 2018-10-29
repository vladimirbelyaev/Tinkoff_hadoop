import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SortJob extends Configured implements Tool {
    public static class SortMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyVal = value.toString().split("\t");
            long val = Long.parseLong(keyVal[1]);
            context.write(new LongWritable(val), new Text(keyVal[0]));
        }
    }



    public static class SortReducer extends Reducer<LongWritable, Text, Text, Text> {
        @Override
        protected void reduce(LongWritable mass, Iterable<Text> links, Context context) throws IOException, InterruptedException {
            for (Text i: links){
                context.write(i, new Text(Long.toString(mass.get())));
            }
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(SortJob.class);
        job.setJobName(SortJob.class.getCanonicalName());

        // will use traditional TextInputFormat to split line-by-line
        KeyValueTextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new SortJob(), args);
        System.exit(ret);
    }
}
