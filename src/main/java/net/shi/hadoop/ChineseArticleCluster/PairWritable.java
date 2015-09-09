package net.shi.hadoop.ChineseArticleCluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableFactories;

public class PairWritable implements WritableComparable<PairWritable> {
	
	private Class<? extends Writable> firstClass, secondClass;
	private Writable first, second;
	
	public PairWritable(PairWritable pair){
		this.first = pair.getFirst();
		this.second = pair.getSecond();
	}
	
	public PairWritable(Class<? extends Writable> firstClass,
			Class<? extends Writable> secondClass){	
		this.firstClass = firstClass;
		this.secondClass = secondClass;
	}
	
	
	public PairWritable(Writable first, Writable second){
		this.first = first;
		this.second = second;
		firstClass = first.getClass();
		secondClass = second.getClass();
	}
	
	public Writable getFirst(){
		return first;
	}
	
	public Writable getSecond(){
		return second;
	}
	
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		first.write(out);
		second.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		Writable value = WritableFactories.newInstance(firstClass);
		value.readFields(in);
		first = value;
		
		value = WritableFactories.newInstance(secondClass);
		value.readFields(in);
		second = value;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public int compareTo(PairWritable pair) {
		// TODO Auto-generated method stub
		int cmp =  ((Comparable) first).compareTo((Comparable) pair.first);
		if(cmp != 0){
			return cmp;
		}
		return ((Comparable) second).compareTo((Comparable) pair.second);
	}
	
	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof PairWritable){
			PairWritable pair = (PairWritable) o;
			return first.equals(pair.first) && second.equals(pair.second);
		}
		return false;
	}
	
	@Override
	public String toString(){
		return first.toString() + ":" + second.toString();
	}
	
	public Text toText(){
		return new Text(toString());
	}
}

class TextPairWritable extends PairWritable{
	public TextPairWritable(){
		super(Text.class, Text.class);
	}
	
	public TextPairWritable(Writable first, Writable second){
		super(first, second);
	}
	
	public TextPairWritable(String first, String second){
		this(new Text(first), new Text(second));
	}
	
	public TextPairWritable(Text first, String second){
		this(first, new Text(second));
	}
	
	public TextPairWritable(String first, Text second){
		this(new Text(first), second);
	}
}

class TextIntPairWritable extends PairWritable{
	public TextIntPairWritable(){
		super(Text.class, IntWritable.class);
	}
	
	public TextIntPairWritable(Writable first, Writable second){
		super(first, second);
	}
	
	public TextIntPairWritable(String first, int second){
		this(new Text(first), new IntWritable(second));
	}
	
	public TextIntPairWritable(Text first, int second){
		this(first, new IntWritable(second));
	}
	
	public TextIntPairWritable(String first, IntWritable second){
		this(new Text(first), second);
	}
}

class IntPairWritable extends PairWritable{
	public IntPairWritable(){
		super(IntWritable.class, IntWritable.class);
	}
	
	public IntPairWritable(Writable first, Writable second){
		super(first, second);
	}
	
	public IntPairWritable(int first, int second){
		this(new IntWritable(second), new IntWritable(second));
	}
	
	public IntPairWritable(IntWritable first, int second){
		this(first, new IntWritable(second));
	}
	
	public IntPairWritable(int first, IntWritable second){
		this(new IntWritable(first), second);
	}
}