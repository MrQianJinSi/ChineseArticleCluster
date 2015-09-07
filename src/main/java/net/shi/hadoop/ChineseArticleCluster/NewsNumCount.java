package net.shi.hadoop.ChineseArticleCluster;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NewsNumCount extends Configured implements Tool {
	private static String INPUT_PATH = "/home/galois/workspace/SougouNewsOutput/combinSequence";
	private static String OUTPUT_PATH = "/home/galois/workspace/SougouNewsOutput/NewsNumCount";
	
	public static class NewsNumCountMapper
	extends Mapper<Text, Text, IntWritable, Text>{
		private static IntWritable one = new IntWritable(1);
		
		@Override
		public void map(Text key, Text value,
				Context context) throws IOException, InterruptedException{
			context.write(one, key);
		}
	}
	
	public static class NewsNumCountReducer
	extends Reducer<IntWritable, Text, LongWritable, NullWritable>{
//		private Text placeholder = new Text("PLACE");
		
		@Override
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException{//
			long sum = 0;
			
			for(@SuppressWarnings("unused") Text val : values)
				sum++;
			
			context.write(new LongWritable(sum), NullWritable.get());
		}
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(getConf(), "News Number Count");
		job.setJarByClass(NewsNumCount.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		job.setMapperClass(NewsNumCountMapper.class);
		job.setReducerClass(NewsNumCountReducer.class);

		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String args[]) throws Exception{
		String[] paths = {INPUT_PATH, OUTPUT_PATH};
		
		int exitCode = ToolRunner.run(new NewsNumCount(), paths);
		
		System.exit(exitCode);
	}
}
