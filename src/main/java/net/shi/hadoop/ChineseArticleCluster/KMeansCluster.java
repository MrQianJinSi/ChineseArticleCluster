package net.shi.hadoop.ChineseArticleCluster;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IOUtils;
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
	extends Mapper<Text, ArrayPrimitiveWritable, Text, ArrayPrimitiveWritable>{
		double[][] centers;
		Text[] centersTag;
		int vetcorSize;
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
				long fileStart = reader.getPosition();//记录文件开始位置
				Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
				ArrayPrimitiveWritable value = (ArrayPrimitiveWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
				int ix = 0;
				while(reader.next(key, value)){
					ix++;
				}
				
				centersNum = ix;
				vetcorSize = ((double []) value.get()).length;
				centers = new double[centersNum][vetcorSize];
				centersTag = new Text[centersNum];
				
				reader.seek(fileStart);
				ix = 0;
				while(reader.next(key, value)){
					centersTag[ix] = new Text(key);
					centers[ix] = ((double []) value.get());
					ix++;
				}
			} finally {
				IOUtils.closeStream(reader);
			}
		}
		
		@Override
		public void map(Text key, ArrayPrimitiveWritable value,
				Context context) throws IOException, InterruptedException{
			double[] vector = (double[]) value.get();
			double minDist = Double.MAX_VALUE;
			int closestIx  = -1;
			for(int i = 0; i < centersNum; ++i){
				double dist = MathVector.getEuclideanDistance(vector, centers[i]);
				if(dist < minDist){
					closestIx = i;
					minDist = dist;
				}
			}
			context.write(centersTag[closestIx], value);
		}
	}
	
	public static class KMeansClusterReducer
	extends Reducer<Text, ArrayPrimitiveWritable, Text, ArrayPrimitiveWritable>{
		public void reduce(Text key, Iterable<ArrayPrimitiveWritable> values,
				Context context) throws IOException, InterruptedException{
			double[] sumCenter = null;
			int vecNum = 0;
			for(ArrayPrimitiveWritable val : values){
				sumCenter = MathVector.addVector(sumCenter, (double[]) val.get());
				vecNum += 1;
			}
			double[] avgCenter = MathVector.dividedByScala(sumCenter, (double) vecNum);
			
			context.write(key, new ArrayPrimitiveWritable(avgCenter));
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
		job.setOutputValueClass(ArrayPrimitiveWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(paths[0]));
		FileOutputFormat.setOutputPath(job, new Path(paths[1]));
		
		return job.waitForCompletion(true)? 0 : 1;
	}

	// 计算两个centers之间的最大距离
	public static double getCentersMaxDiff(Configuration conf, Path oldPath, Path newPath) throws IOException{		
		FileSystem fs = oldPath.getFileSystem(conf);
		
		//读取旧的中心文件
		SequenceFile.Reader oldFileReader = null;
		Map<Text, ArrayPrimitiveWritable> oldData = new HashMap<>();
		try {
			oldFileReader = new SequenceFile.Reader(fs, oldPath, conf);
			Text key = (Text) ReflectionUtils.newInstance(oldFileReader.getKeyClass(), conf);
			ArrayPrimitiveWritable value = (ArrayPrimitiveWritable) ReflectionUtils.newInstance(oldFileReader.getValueClass(), conf);
			while(oldFileReader.next(key, value)){
				oldData.put(new Text(key), new ArrayPrimitiveWritable(value.get()));
			}
		} finally {
			IOUtils.closeStream(oldFileReader);
		}
		//读取新的中心文件
		SequenceFile.Reader newFileReader = null;
		Map<Text, ArrayPrimitiveWritable> newData = new HashMap<>();
		try {
			newFileReader = new SequenceFile.Reader(fs, newPath, conf);
			Text key = (Text) ReflectionUtils.newInstance(newFileReader.getKeyClass(), conf);
			ArrayPrimitiveWritable value = (ArrayPrimitiveWritable) ReflectionUtils.newInstance(newFileReader.getValueClass(), conf);
			while(newFileReader.next(key, value)){
				newData.put(new Text(key), new ArrayPrimitiveWritable(value.get()));
			}
		} finally {
			IOUtils.closeStream(newFileReader);
		}
		
		double maxDiff = 0;
		Set<Text> centerTags = newData.keySet();
		Iterator<Text> iter = centerTags.iterator();
		while(iter.hasNext()){
			Text key = iter.next();
			double[] oldCenter = (double[]) oldData.get(key).get();
			double[] newCenter = (double[]) newData.get(key).get();
			double dist = MathVector.getEuclideanDistance(oldCenter, newCenter);
			if(dist > maxDiff)
				maxDiff = dist;
		}
		
		return maxDiff;
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String[] paths = new String[2];
		paths[0] = INPUT_PATH;
		
		int iterNum = 0;
		double threshold = 20;
		
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
		}while(iterNum < 10);
		
		log.close();
		
		System.exit(exitCode);
	}

}
