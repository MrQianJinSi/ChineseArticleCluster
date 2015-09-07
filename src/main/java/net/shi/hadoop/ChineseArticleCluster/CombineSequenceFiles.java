package net.shi.hadoop.ChineseArticleCluster;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CombineSequenceFiles extends Configured implements Tool {
	private static String INPUT_PATH_PREFIX = "/home/galois/workspace/SougouNewsOutput/";
	private static String[] SUFFIX_SET = {"08", "10", "13", "14", "16", "20", "22", "23", "24"};
	private static String OUTPUT_PATH = "/home/galois/workspace/SougouNewsOutput/combinSequence";

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(getConf(), "Combine Sequence Files");
		job.setJarByClass(CombineSequenceFiles.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		int len = args.length;
		for(int i = 0; i < len-1; ++i)
			FileInputFormat.addInputPath(job, new Path(args[i]));
		FileOutputFormat.setOutputPath(job, new Path(args[len-1]));
		
		return job.waitForCompletion(true)? 0 : 1;
	}
	
	public static void main(String args[]) throws Exception{
		String[] paths = new String[SUFFIX_SET.length + 1];
		for(int i = 0; i < SUFFIX_SET.length; ++i)
			paths[i] = INPUT_PATH_PREFIX + SUFFIX_SET[i];
		paths[SUFFIX_SET.length] = OUTPUT_PATH;
		
		int exitCode = ToolRunner.run(new CombineSequenceFiles(), paths);
		
		System.exit(exitCode);
	}

}
