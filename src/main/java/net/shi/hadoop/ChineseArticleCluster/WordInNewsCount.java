package net.shi.hadoop.ChineseArticleCluster;

import java.io.IOException;
import java.util.List;
import java.util.ListIterator;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordInNewsCount extends Configured implements Tool {
	private static final String INPUT_PATH = "/home/galois/workspace/SougouNewsInput/C000008_bak";
	private static final String OUTPUT_PATH = "/home/galois/workspace/SougouNewsOutput/wordCount";
	
	public static class WordInNewsCountMapper
	extends Mapper<Text, Text, Text, IntWritable>{
		private IntWritable one = new IntWritable(1);
		
		public void map(Text key, Text value,
				Context context) throws IOException, InterruptedException{
	    	List<Term> termList = ToAnalysis.parse(value.toString());
	    	ListIterator<Term> iter = termList.listIterator();
	    	
	    	while(iter.hasNext()){
	    		String outputKey = iter.next() + "@" + key;
	    		context.write(new Text(outputKey), one);
	    	}
		}
	}

	public static class WordInNewsCountReducer
	extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException{
			int count = 0;
			
			for(IntWritable value : values){
				count += value.get();
			}
			context.write(key, new IntWritable(count));
		}
	}
	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(getConf(), "Word In News Count");
		job.setJarByClass(WordInNewsCount.class);
		
		job.setInputFormatClass(NewsInputFormat.class);
		
		job.setMapperClass(WordInNewsCountMapper.class);
		job.setReducerClass(WordInNewsCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode = ToolRunner.run(new WordInNewsCount(), args);
		
		System.exit(exitCode);
	}

}
