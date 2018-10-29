import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URL;

public class QueryCountJob extends Configured implements Tool {
    public static class QueryCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        static final IntWritable one = new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] partsOfData = value.toString().split("\t");
            URL url;
            try {
                url = new URL(partsOfData[0]);
            }catch (Exception ex){
                System.out.println(partsOfData[0]);
                return;
            }
            String queryPart = url.getQuery();
            if (queryPart == null){
                return;
            }
            String[] partsOfQuery = queryPart.split("&");
            for (String part : partsOfQuery) {
                String[] keyVal = part.split("=");
                if (keyVal.length != 2){
                    System.out.println(part);
                    return;
                }
                if (keyVal[0].equals("") | keyVal[1].equals("")){
                    System.out.println(part);
                    return;
                }
                context.write(new Text(keyVal[0]), one);
            }
        }
    }


    public static class QueryCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text word, Iterable<IntWritable> nums, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable i: nums) {
                sum += i.get();
            }
            context.write(word, new IntWritable(sum)); // То, что в коде лекций emit
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(QueryCountJob.class);
        job.setJobName(QueryCountJob.class.getCanonicalName());

        job.setNumReduceTasks(5);
        TextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(QueryCountMapper.class);
        job.setReducerClass(QueryCountReducer.class);

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
        int ret = ToolRunner.run(new QueryCountJob(), args);
        System.exit(ret);
    }
}
