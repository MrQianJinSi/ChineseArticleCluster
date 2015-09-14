package net.shi.hadoop.ChineseArticleCluster;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
public class WordInNewsTFIDF extends Configured implements Tool {
	private static final String INPUT_PATH = "/home/galois/workspace/SougouNewsOutput/wordFrequency";
	private static final String OUTPUT_PATH = "/home/galois/workspace/SougouNewsOutput/wordTFIDF";
	private static final String NEWS_NUM_PATH = "/home/galois/workspace/SougouNewsOutput/NewsNumCount/part-r-00000";
	
	private static class TextIntIntPair extends PairWritable{
		public TextIntIntPair(){
			super(Text.class, IntPairWritable.class);
		}
		
		public TextIntIntPair(Writable first, Writable second){
			super(first, second);
		}
		
		@SuppressWarnings("unused")
		public TextIntIntPair(String first, IntPairWritable second){
			this(new Text(first), second);
		}
	}
	
	public static class WordInNewsTFIDFMapper
	extends Mapper<TextPairWritable, IntPairWritable, Text, TextIntIntPair>{
		
		public void map(TextPairWritable key, IntPairWritable value,
				Context context) throws IOException, InterruptedException{
			Text word = (Text) key.getFirst();
			TextIntIntPair pair = new TextIntIntPair((Text) key.getSecond(), value);
			context.write(word, pair);
		}
	}

	public static class WordInNewsTFIDFReducer
	extends Reducer<Text, TextIntIntPair, TextPairWritable, DoubleWritable>{
		int newsNumCount = 0;
		
		@Override
		public void setup(Context context) throws IOException{
			//读取News Num
			Path[] caches = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			
			if (caches == null || caches.length <= 0) {
				System.err.println("News Number file does not exist");
				System.exit(1);
			}
			
			//此处不能直接使用java的FileReader，因为MapReduce框架会自动给文件路径加前缀file:
			//这样会导致FileReader不能识别路径，建议使用MapReduce自带的文件读取API
			//BufferedReader in = new BufferedReader(new FileReader(caches[0].toString()));
			
			Path newsNumPath = caches[0];
			FileSystem fs = newsNumPath.getFileSystem(context.getConfiguration());
			BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(newsNumPath)));
			try{
				String line = in.readLine();
				newsNumCount = Integer.parseInt(line);
			}
			finally{
				in.close();
			}			
		}
		
		public void reduce(Text key, Iterable<TextIntIntPair> values,
				Context context) throws IOException, InterruptedException{
			int newsNumIncludeWord = 0;
			List<TextIntIntPair> pairList = new LinkedList<>();
			for(TextIntIntPair value : values){
				newsNumIncludeWord++;
				pairList.add(new TextIntIntPair(value.getFirst(), value.getSecond()));
			}
			
			double idf = (double) newsNumCount / newsNumIncludeWord;
			idf = Math.log10(idf);
			Iterator<TextIntIntPair> iter = pairList.iterator();
			
			TextIntIntPair pair;
			while(iter.hasNext()){
				pair = iter.next();
				TextPairWritable outKey = new TextPairWritable(key, pair.getFirst());
				IntPairWritable tfPair = (IntPairWritable) pair.getSecond();
				double tf = (double) ((IntWritable) tfPair.getFirst()).get() / 
						((IntWritable) tfPair.getSecond()).get();
				double tfidf = tf*idf;
				context.write(outKey, new DoubleWritable(tfidf));
			}
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = this.getConf();
		Path newsNum = new Path(args[2]);
		DistributedCache.addCacheFile(newsNum.toUri(), conf); //放在new Job之前，否则读不到CacheFile
		
		Job job = Job.getInstance(conf, "Get Words' TFIDF");
		job.setJarByClass(WordInNewsTFIDF.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapperClass(WordInNewsTFIDFMapper.class);
		job.setReducerClass(WordInNewsTFIDFReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextIntIntPair.class);
		
		job.setOutputKeyClass(TextPairWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setNumReduceTasks(9);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String[] paths = {INPUT_PATH, OUTPUT_PATH, NEWS_NUM_PATH};
		int exitCode = ToolRunner.run(new WordInNewsTFIDF(), paths);
		
		System.exit(exitCode);
	}
}
