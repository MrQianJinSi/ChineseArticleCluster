package net.shi.hadoop.ChineseArticleCluster;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NewsToSequenceFileConvertor extends Configured implements Tool {
	private static String INPUT_PATH_PREFIX = "/home/galois/workspace/SougouNewsInput/C0000";
	private static String[] SUFFIX_SET = {"08", "10", "13", "14", "16", "20", "22", "23", "24"};
	private static String OUTPUT_PATH = "/home/galois/workspace/SougouNewsOutput/";
	
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
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String args[]) throws Exception{
		String[] paths = new String[2];

		int exitCode = 1;
		for(int i = 0; i < SUFFIX_SET.length; ++i){
			paths[0] = INPUT_PATH_PREFIX + SUFFIX_SET[i];
			paths[1] = OUTPUT_PATH + SUFFIX_SET[i];

			exitCode = ToolRunner.run(new NewsToSequenceFileConvertor(), paths);
			
			if(exitCode != 0)
				break;
		}

		System.exit(exitCode);
	}
}
