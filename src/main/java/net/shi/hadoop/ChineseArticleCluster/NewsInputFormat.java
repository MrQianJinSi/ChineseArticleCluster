package net.shi.hadoop.ChineseArticleCluster;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class NewsInputFormat extends FileInputFormat<Text, Text> {

	@Override
	protected boolean isSplitable(JobContext context, Path file){
		return false;
	}

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		WholeFileReader reader = new WholeFileReader();
		reader.initialize(split, context);
		return reader;
	}
}

class WholeFileReader extends RecordReader<Text, Text>{
	private FileSplit fileSplit;
	private Configuration job;
	private boolean isRead = false;

	private BufferedReader in;

	private Text key = new Text();
	private Text value = new Text();
	
	private static String[] META_CHARS = { " "};

	private static String[] META_CHARS_SERIALIZATIONS = { "&nbsp" };

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		in.close();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(isRead)
			return 1;
		else
			return 0;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		this.fileSplit = (FileSplit) split;
		this.job = context.getConfiguration();

		Path path = fileSplit.getPath();
		FileSystem fs = path.getFileSystem(job);
		
		String[] dirs = path.toString().split(java.io.File.separator);
		int sepNum = dirs.length;
		key = new Text(dirs[sepNum-2] + "_" + dirs[sepNum-1]);

		in = new BufferedReader(new InputStreamReader(fs.open(path)));
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String body = "";
		String line;
		if(!isRead){
			while((line = in.readLine()) != null){
				body = body + line;
			}
			
			for(int i = 0; i < META_CHARS_SERIALIZATIONS.length; ++i){
				body = body.replaceAll(META_CHARS_SERIALIZATIONS[i], META_CHARS[i]);
			}
			value = new Text(body);
			
			isRead = true;
			return true;
		}
		return false;
	}
}

