package net.shi.hadoop.ChineseArticleCluster;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
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

@SuppressWarnings("deprecation")
public class RandomInitCenters extends Configured implements Tool {
	private static final String INPUT_PATH = "/home/galois/workspace/SougouNewsOutput/newsVector";
	private static final String OUTPUT_PATH = "/home/galois/workspace/SougouNewsOutput/centers/iter0";
	private static final String NEWS_ID_PATH = "/home/galois/workspace/SougouNewsOutput/newsIdSet/part-r-00000";
	
	public static class RandomInitCentersMapper
	extends Mapper<Text, MapWritable, Text, MapWritable>{
		private Set<Text> centersList = new HashSet<>();
//		private Text placeholder = new Text("PLACEHOLDER");
		
		public void setup(Context context) throws IOException{
			//读取存储的centers
			Configuration conf = context.getConfiguration();
			Path[] caches = DistributedCache.getLocalCacheFiles(conf);
			
			if (caches == null || caches.length <= 0) {
				System.err.println("Centers Index File does not exist");
				System.exit(1);
			}
			
			//此处不能直接使用java的FileReader，因为MapReduce框架会自动给文件路径加前缀file:
			//这样会导致FileReader不能识别路径，建议使用MapReduce自带的文件读取API
			//BufferedReader in = new BufferedReader(new FileReader(caches[0].toString()));
			Path centerIndexFile = caches[0];
			FileSystem fs = centerIndexFile.getFileSystem(conf);
			String line;
			BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(centerIndexFile)));
			try{
				while((line = in.readLine()) != null)
					centersList.add(new Text(line));
			}
			finally{
				in.close();
			}
		}
		
		public void map(Text key, MapWritable value,
				Context context) throws IOException, InterruptedException{

			if(centersList.contains(key)){
				context.write(key, value);
			}
		}
	}
	
//	public static class RandomInitCentersReducer
//	extends Reducer<Text, MapWritable, Text, MapWritable>{
//		
//		public void reduce(Text key, Iterable<MapWritable> values,
//				Context context) throws IOException, InterruptedException{
//			for(MapWritable val : values)
//				context.write(key, val);
//		}
//	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = this.getConf();
		conf.setInt("net.shi.cluster.size", Integer.parseInt(args[3]));
		
		int ix = OUTPUT_PATH.lastIndexOf(File.separator) + 1;
		String centersIxPath = OUTPUT_PATH.substring(0, ix) + "tmp/random";
		getInitialCenters(args[2], centersIxPath);
		Path centers = new Path(centersIxPath);
		DistributedCache.addCacheFile(centers.toUri(), conf); //放在new Job之前，否则读不到CacheFile
		
		Job job = Job.getInstance(conf, "Initialize Centers");
		job.setJarByClass(RandomInitCenters.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapperClass(RandomInitCentersMapper.class);
		job.setReducerClass(Reducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);
				
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	private void getInitialCenters(String newsID, String outPath) throws IOException{
		Configuration conf = this.getConf();
		//读取新闻数量
		Path newsIDPath = new Path(newsID);
		FileSystem fs = newsIDPath.getFileSystem(conf);
		BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(newsIDPath)));
		List<String>  newsIdSet = new ArrayList<>();
		try{
			String line;
			while((line = in.readLine()) != null)
				newsIdSet.add(line);
		}
		finally{
			in.close();
		}	
		long newsNumCount = newsIdSet.size();
		
		//随机分配中心
		Set<Long> centerIxList = new HashSet<>();//随机分配第一次迭代的中心
		int clusterSize = conf.getInt("net.shi.cluster.size", 0);
		int ix = 0;
		PrintWriter out = new PrintWriter(outPath);
		while(ix < clusterSize){
			long id = Math.round(Math.random()*newsNumCount);
			if(centerIxList.contains(id))
				continue;
			centerIxList.add(id);
			out.println(newsIdSet.get((int) id));
			ix++;
		}	
		out.close();
		System.out.println(centerIxList);
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String[] paths = {INPUT_PATH, OUTPUT_PATH, NEWS_ID_PATH, "9"};
		
		int exitCode = ToolRunner.run(new RandomInitCenters(), paths);
		
		System.exit(exitCode);
	}

}
