package assignment3;

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

import java.io.IOException;
import java.util.*;

public class WordCount {

    public static void main(String[] args) throws Exception {

        // create a configuration for hadoop
        Configuration config = new Configuration();

        // parse passed arguments
        String[] arguments = new GenericOptionsParser(config, args).getRemainingArgs();

        if(arguments.length != 3) {
            System.out.println("program requires three command line arguments");
            System.out.println("1. input text file");
            System.out.println("2. output file");
            System.out.println("3. Integer (will print out top N most popular words)");
        }

        // input book
        Path input = new Path(arguments[0]);

        // output result file
        Path output = new Path(arguments[1]);

        // set top N
        config.set("N", arguments[2]);

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
                if(token.length() >= 3) {
                    word = token;
                    con.write(new Text(word), new IntWritable(1));
                }
            }
        }
    }

    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private Map<Text, IntWritable> wordSumMap = new HashMap<Text, IntWritable>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            // count the occurrences of each word
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            // save the word and its occurrences to a map to be sorted
            wordSumMap.put(new Text(key), new IntWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            // sort the words so the most popular ones are at the top
            Map<Text, IntWritable> topWords = sortByValueDescending(wordSumMap);

            int count = 0;

            // retrieve amount of words to print out from the configuration
            Configuration config = context.getConfiguration();
            int N = Integer.parseInt(config.get("N"));

            // print out the top N words
            for (Text word : topWords.keySet()) {

                // break when we hit N
                if (count == N)
                    break;
                context.write(word, topWords.get(word));
                count++;
            }
        }

        /***********************************************************************************************
        *    Title: Sort Map by Value
        *    Author: Carter Page
        *    Date: Aug 26, 2016
        *    Code version: 8
        *    Availability: http://stackoverflow.com/questions/109383/sort-a-mapkey-value-by-values-java
        ************************************************************************************************/
        private static <K, V extends Comparable<? super V>> Map<K, V> sortByValueDescending(Map<K, V> map)
        {
            List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());

            Collections.sort( list, new Comparator<Map.Entry<K, V>>()
            {
                public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
                {
                    return (o1.getValue()).compareTo( o2.getValue() );
                }
            });

            // reverse list so it is descending
            Collections.reverse(list);

            Map<K, V> result = new LinkedHashMap<K, V>();
            for (Map.Entry<K, V> entry : list)
            {
                result.put( entry.getKey(), entry.getValue() );
            }
            return result;
        }
    }
}