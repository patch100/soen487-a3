package assignment3;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

    public static void main(String[] args) throws Exception {

        // create a configuration for hadoop
        Configuration config = new Configuration();

        // parse passed arguments
        String[] files = new GenericOptionsParser(config, args).getRemainingArgs();

        // input book
        Path input = new Path(files[0]);

        // output result file
        Path output = new Path(files[1]);

        // prepare job
        Job job = new Job(config, "assignment3");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MapForWordCount.class);
        job.setReducerClass(ReduceForWordCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // prepare input/output/exit
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String word;
            String line = value.toString();

            for(String token : line.split("[^\\w']+")) {
                if(token.length() > 3) {
                    word = token;
                    con.write(new Text(word), new IntWritable(1));
                }
            }
        }
    }

    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce (Text word, Iterable < IntWritable > values, Context con)throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            con.write(word, new IntWritable(sum));
        }
    }
}