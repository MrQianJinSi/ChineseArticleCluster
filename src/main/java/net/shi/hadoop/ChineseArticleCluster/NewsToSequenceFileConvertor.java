package net.shi.hadoop.ChineseArticleCluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

public class NewsToSequenceFileConvertor extends Configured implements Tool {

	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Combine Small Files");
		
//		job.setInputFormatClass(cls);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
