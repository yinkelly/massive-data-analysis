import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class WordCount {
        
 public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            String temp = tokenizer.nextToken();
            if (temp.length() == 7){
                word.set(temp);
                context.write(word, one);
            }
        }
    }
 } 
        
 public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        if (sum>100){
            context.write(key, new IntWritable(sum));
        }
    }
 }
  
 public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
       {
            int number = 999;
            String word = "empty";

            if (tokenizer.hasMoreTokens()) {
                String str0 = tokenizer.nextToken();
                word = str0.trim();
            }

            if (tokenizer.hasMoreElements()) {
                String str1 = tokenizer.nextToken();
                number = Integer.parseInt(str1.trim());
            }
            context.write(new IntWritable(number), new Text(word));
        }
    }
 } 
        
 public static class Reduce2 extends Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        int counter = 0;
        for (Text val : values) {
            if (counter<100) {
                context.write(key, val);
                counter++;
            }
        }
    }
 }       
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "wordcount");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map1.class);
    job.setReducerClass(Reduce1.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(WordCount.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path("../tmp/temp/"));


    Job job2 = new Job(conf, "wordcount");

    // job2.setOutputKeyClass(Text.class);
    // job2.setOutputValueClass(IntWritable.class);
    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(Text.class);

    job2.setMapperClass(Map2.class);
    job2.setReducerClass(Reduce2.class);

    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    job2.setNumReduceTasks(1);
    job2.setJarByClass(WordCount.class);

    FileInputFormat.setInputPaths(job2, new Path("../tmp/temp/part-r-00000"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        
    // job.waitForCompletion(true);

    job.submit();
    if (job.waitForCompletion(true)) {
        job2.submit();
        job2.waitForCompletion(true);
    }

 }
        
}