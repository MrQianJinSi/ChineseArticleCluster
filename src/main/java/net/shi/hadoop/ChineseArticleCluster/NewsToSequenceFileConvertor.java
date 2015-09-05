package net.shi.hadoop.ChineseArticleCluster;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NewsToSequenceFileConvertor extends Configured implements Tool {
	private static String INPUT_PATH = "/home/galois/Downloads/SogouC.reduced/Reduced/C000008_bak";
	private static String OUTPUT_PATH = "/home/galois/workspace/ChineseNewsOutput";
	
//	public static class NewsToSequenceFileConvertorMapper 
//	extends Mapper<Text, Text, Text, Text>{
//		public
//	}
	
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Combine Small Files");
		
		job.setJarByClass(NewsToSequenceFileConvertor.class);		
		job.setInputFormatClass(NewsInputFormat.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String args[]) throws Exception{
		String[] paths = {INPUT_PATH, OUTPUT_PATH};
		
		int exitCode = ToolRunner.run(new NewsToSequenceFileConvertor(), paths);
		
		System.exit(exitCode);
	}
}
