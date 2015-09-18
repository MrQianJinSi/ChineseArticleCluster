package net.shi.hadoop.ChineseArticleCluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

@SuppressWarnings("deprecation")
public class GetNewsCluster extends Configured implements Tool {
	private static final String INPUT_PATH = "/home/galois/workspace/SougouNewsOutput/newsVector";
	private static final String OUTPUT_PATH = "/home/galois/workspace/SougouNewsOutput/cluster_bak";
	private static final String CENTERS_PATH = "/home/galois/workspace/SougouNewsOutput/centers_bak/iter24/part-r-00000";
	
	public static class GetNewsClusterMapper
	extends Mapper<Text, MapWritable, Text, Text>{
		List<MapWritable> centers = new ArrayList<>();
		List<Text> centersTag = new ArrayList<>();
		int centersNum;
		
		@Override
		public void setup(Context context) throws IOException{
			Configuration conf = context.getConfiguration();
			Path[] caches = DistributedCache.getLocalCacheFiles(conf);
			Path centersFile = caches[0];
			
			FileSystem fs = FileSystem.get(centersFile.toUri(), conf);
			SequenceFile.Reader reader = null;
			try {
				reader = new SequenceFile.Reader(fs, centersFile, conf);
				Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
				MapWritable value = (MapWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
				while(reader.next(key, value)){
					centersTag.add(new Text(key));
					centers.add(new MapWritable(value));
				}
				
				centersNum = centers.size();
			} finally {
				IOUtils.closeStream(reader);
			}
		}
		
		public void map(Text key, MapWritable value,
				Context context) throws IOException, InterruptedException{
			double minDist = Double.MAX_VALUE;
			int closestIx  = -1;
			for(int i = 0; i < centersNum; ++i){
				double dist = MathVector.getEuclideanDistance(value, centers.get(i));
				if(dist < minDist){
					closestIx = i;
					minDist = dist;
				}
			}
			context.write(centersTag.get(closestIx), key);
		}
	}
	
	public static class GetNewsClusterReducer
	extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException{
			String text = "[";
			for(Text val : values){
				text = text + val.toString() + ",";
			}
			text += "]";
			context.write(key, new Text(text));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = this.getConf();
		Path centers = new Path(args[2]);
		DistributedCache.addCacheFile(centers.toUri(), conf); //放在new Job之前，否则读不到CacheFile
		
		Job job = Job.getInstance(conf, "Get News' Cluster");
		job.setJarByClass(GetNewsCluster.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(GetNewsClusterMapper.class);
		job.setReducerClass(GetNewsClusterReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String[] paths = {INPUT_PATH, OUTPUT_PATH, CENTERS_PATH};
		
		int exitCode = ToolRunner.run(new GetNewsCluster(), paths);
		
		System.exit(exitCode);
	}

}
