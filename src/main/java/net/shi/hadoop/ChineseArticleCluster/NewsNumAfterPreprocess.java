package net.shi.hadoop.ChineseArticleCluster;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

//在进行分词后，部分非中文新闻就被排除了，因此，这里需要重新统计中文新闻的数量。
public class NewsNumAfterPreprocess extends Configured implements Tool {
	private static final String INPUT_PATH = "/home/galois/workspace/SougouNewsOutput/wordCount";
	private static final String OUTPUT_PATH = "/home/galois/workspace/SougouNewsOutput/newsNumAfterPreprocess";
	
	public static class NewsNumAfterPreprocessMapper
	extends Mapper<TextPairWritable, IntWritable, IntWritable, Text>{
		private IntWritable one = new IntWritable(1);
		public void map(TextPairWritable key, IntWritable value,
				Context context) throws IOException, InterruptedException{
			context.write(one, (Text) key.getSecond());
		}
	}
	
	public static class NewsNumAfterPreprocessReducer
	extends Reducer<IntWritable, Text, IntWritable, NullWritable>{
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException{
			Set<Text> newsIdSet = new HashSet<>();
			for(Text val : values){
				if(newsIdSet.contains(val))
					continue;
				newsIdSet.add(val);
			}
			context.write(new IntWritable(newsIdSet.size()), NullWritable.get());
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(this.getConf(), "News Num Count After Preprocess");
		job.setJarByClass(NewsNumAfterPreprocess.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapperClass(NewsNumAfterPreprocessMapper.class);
		job.setReducerClass(NewsNumAfterPreprocessReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String[] paths = {INPUT_PATH, OUTPUT_PATH};
		
		int exitCode = ToolRunner.run(new NewsNumAfterPreprocess(), paths);
		
		System.exit(exitCode);
	}

}
