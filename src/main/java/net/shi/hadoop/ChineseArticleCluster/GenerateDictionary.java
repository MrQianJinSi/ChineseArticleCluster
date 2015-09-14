package net.shi.hadoop.ChineseArticleCluster;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class GenerateDictionary extends Configured implements Tool {
	private static final String INPUT_PATH = "/home/galois/workspace/SougouNewsOutput/wordTFIDF";
	private static final String OUTPUT_PATH = "/home/galois/workspace/SougouNewsOutput/dictionary";
	
	public static class GenerateDictionaryMapper
	extends Mapper<TextPairWritable, DoubleWritable, Text, NullWritable>{
		public void map(TextPairWritable key, DoubleWritable value,
				Context context) throws IOException, InterruptedException{
			context.write((Text) key.getFirst(), NullWritable.get());
		}
	}
	
	public static class GenerateDictionaryReducer
	extends Reducer<Text, NullWritable, Text, NullWritable>{
		public void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException{
			context.write(key, NullWritable.get());
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(getConf(), "Generate Dictionary");
		job.setJarByClass(GenerateDictionary.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		job.setMapperClass(GenerateDictionaryMapper.class);
		job.setReducerClass(GenerateDictionaryReducer.class);
			
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		String[] paths = {INPUT_PATH, OUTPUT_PATH};
		
		int exitCode = ToolRunner.run(new GenerateDictionary(), paths);
		
		System.exit(exitCode);
	}

}
