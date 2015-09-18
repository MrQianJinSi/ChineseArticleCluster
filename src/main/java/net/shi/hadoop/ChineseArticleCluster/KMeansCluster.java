package net.shi.hadoop.ChineseArticleCluster;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

@SuppressWarnings("deprecation")
public class KMeansCluster{
	private static final String INPUT_PATH = "/home/galois/workspace/SougouNewsOutput/newsVector";
	private static final String OUTPUT_PATH = "/home/galois/workspace/SougouNewsOutput/centers/";
	
	public static class KMeansClusterMapper
	extends Mapper<Text, MapWritable, Text, MapWritable>{
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
		
		@Override
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
			context.write(centersTag.get(closestIx), value);
		}
	}
	
	public static class KMeansClusterReducer
	extends Reducer<Text, MapWritable, Text, MapWritable>{
		public void reduce(Text key, Iterable<MapWritable> values,
				Context context) throws IOException, InterruptedException{
			MapWritable sumCenter = new MapWritable();
			int vecNum = 0;
			for(MapWritable val : values){
				sumCenter = MathVector.addVector(sumCenter, val);
				vecNum += 1;
			}
			MapWritable avgCenter = MathVector.dividedByScala(sumCenter, (double) vecNum);
			
			context.write(key, avgCenter);
		}
	}
	
	public static int doIteration(Configuration conf, String[] paths) throws Exception {
		Job job = Job.getInstance(conf, "K Means Cluster");
		
		job.setJarByClass(KMeansCluster.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapperClass(KMeansClusterMapper.class);
		job.setReducerClass(KMeansClusterReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(paths[0]));
		FileOutputFormat.setOutputPath(job, new Path(paths[1]));
		
		return job.waitForCompletion(true)? 0 : 1;
	}

	// 计算两个centers之间的最大距离
	public static double getCentersMaxDiff(Configuration conf, Path oldPath, Path newPath) throws IOException{		
		FileSystem fs = oldPath.getFileSystem(conf);
		
		//读取旧的中心文件
		SequenceFile.Reader oldFileReader = null;
		Map<Text, MapWritable> oldData = new HashMap<>();
		try {
			oldFileReader = new SequenceFile.Reader(fs, oldPath, conf);
			Text key = (Text) ReflectionUtils.newInstance(oldFileReader.getKeyClass(), conf);
			MapWritable value = (MapWritable) ReflectionUtils.newInstance(oldFileReader.getValueClass(), conf);
			while(oldFileReader.next(key, value)){
				oldData.put(new Text(key), new MapWritable(value));
			}
		} finally {
			IOUtils.closeStream(oldFileReader);
		}
		//读取新的中心文件
		SequenceFile.Reader newFileReader = null;
		Map<Text, MapWritable> newData = new HashMap<>();
		try {
			newFileReader = new SequenceFile.Reader(fs, newPath, conf);
			Text key = (Text) ReflectionUtils.newInstance(newFileReader.getKeyClass(), conf);
			MapWritable value = (MapWritable) ReflectionUtils.newInstance(newFileReader.getValueClass(), conf);
			while(newFileReader.next(key, value)){
				newData.put(new Text(key), new MapWritable(value));
			}
		} finally {
			IOUtils.closeStream(newFileReader);
		}
		
		double maxDiff = 0;
		Set<Text> centerTags = newData.keySet();
		Iterator<Text> iter = centerTags.iterator();
		while(iter.hasNext()){
			Text key = iter.next();
			double dist = MathVector.getEuclideanDistance(oldData.get(key), newData.get(key));
			if(dist > maxDiff)
				maxDiff = dist;
		}
		
		return maxDiff;
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String[] paths = new String[2];
		paths[0] = INPUT_PATH;
		
		int iterNum = 20;
		double threshold = 0.000001;
		
		PrintWriter log = new PrintWriter(OUTPUT_PATH+"log");

		
		int exitCode;
		do{
			Configuration conf = new Configuration();
			Path center = new Path(
					OUTPUT_PATH + "iter" + iterNum + "/part-r-00000"); //老的聚类中心
			DistributedCache.addCacheFile(center.toUri(), conf); //放在new Job之前，否则读不到CacheFile
			
			paths[1] = OUTPUT_PATH + "iter" + (iterNum+1); //输出文件为新的聚类中心
			exitCode = doIteration(conf, paths);
			
			if(exitCode == 1){
				System.err.println("Something bad happend");
				break;
			}
			
			//聚类中心已经收敛
			double maxDiff = getCentersMaxDiff(conf, center, new Path(paths[1]+"/part-r-00000"));
			log.println("iter" + (iterNum+1) + ": " + maxDiff);
			if(maxDiff < threshold)
				break;
			
			iterNum++;
		}while(iterNum < 25);
		
		log.close();
		
		System.exit(exitCode);
	}

}
