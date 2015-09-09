package net.shi.hadoop.ChineseArticleCluster;


import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordInNewsFrequency extends Configured implements Tool {
	private static final String INPUT_PATH = "/home/galois/workspace/SougouNewsOutput/wordCount";
	private static final String OUTPUT_PATH = "/home/galois/workspace/SougouNewsOutput/wordFrequency";
	
	public static class WordInNewsFrequencyMapper
	extends Mapper<TextPairWritable, IntWritable, Text, TextIntPairWritable>{
		public void map(TextPairWritable key, IntWritable value,
				Context context) throws IOException, InterruptedException{
			Text newsId = (Text) key.getSecond();
			Text word = (Text) key.getFirst();
			context.write(newsId, new TextIntPairWritable(word, value));
		}
	}
	
	
	public static class WordInNewsFrequencyReducer
	extends Reducer<Text, TextIntPairWritable, TextPairWritable, IntPairWritable>{
		public void reduce(Text key, Iterable<TextIntPairWritable> values,
				Context context) throws IOException, InterruptedException{
			int totalWordNum = 0;
			List<TextIntPairWritable> wordAndCountList = new LinkedList<>();
			for(TextIntPairWritable value : values){
				IntWritable eachWordNum =  (IntWritable) value.getSecond();
				totalWordNum += eachWordNum.get();
				wordAndCountList.add(new TextIntPairWritable(value.getFirst().toString(), 
						eachWordNum.get())); //必须这么处理，因为value的值会被改写，导致list最后的值都一样。
			}
			
			Iterator<TextIntPairWritable> iter = wordAndCountList.iterator();
			while(iter.hasNext()){
				TextIntPairWritable term = iter.next();
				TextPairWritable outKey = new TextPairWritable(term.getFirst(), key);
				IntPairWritable outValue = new IntPairWritable((IntWritable) term.getSecond(), totalWordNum);
				context.write(outKey, outValue);
			}
			
		}
	}
	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(getConf(), "Word In News Frequency");
		job.setJarByClass(WordInNewsFrequency.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapperClass(WordInNewsFrequencyMapper.class);
		job.setReducerClass(WordInNewsFrequencyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextIntPairWritable.class);
		
		job.setOutputKeyClass(TextPairWritable.class);
		job.setOutputValueClass(IntPairWritable.class);
		
		job.setNumReduceTasks(9);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String[] paths = {INPUT_PATH, OUTPUT_PATH};
		int exitCode = ToolRunner.run(new WordInNewsFrequency(), paths);
		
		System.exit(exitCode);
	}

}
