package net.shi.hadoop.ChineseArticleCluster;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class MathVector {
	//两个向量相加
	public static double[] addVector(double[] vector1, double[] vector2){
		if(vector1 == null & vector2 != null)
			return vector2;
		
		if(vector1 != null & vector2 == null)
			return vector1;
		
		if(vector1.length != vector2.length)
			throw new IllegalArgumentException("two vector's size not equal!");
		
		double[] sumVector = new double[vector1.length];
		for(int i = 0; i < vector1.length; i++)
			sumVector[i] = vector1[i] + vector2[i];
		return sumVector;
	}
	//两个稀疏向量相加
	public static MapWritable addVector(MapWritable vector1, MapWritable vector2){
		if(vector1 == null & vector2 != null)
			return vector2;
		
		if(vector1 != null & vector2 == null)
			return vector1;
		
		Set<Writable> keySet = new HashSet<>(vector1.keySet());
		Set<Writable> keySet1 = vector1.keySet();
		Set<Writable> keySet2 = vector2.keySet();
		keySet.addAll(keySet2);
		Iterator<Writable> iter =  keySet.iterator();
		MapWritable ret = new MapWritable();
		double val1, val2;
		while(iter.hasNext()){
			IntWritable ix = (IntWritable) iter.next();
			val1 = val2 = 0;
			if(keySet1.contains(ix))
				val1 = ((DoubleWritable) vector1.get(ix)).get();
			if(keySet2.contains(ix))
				val2 = ((DoubleWritable) vector2.get(ix)).get();
			ret.put(ix, new DoubleWritable(val1+val2));
		}
		return ret;
	}
	
	//向量除以标量
	public static MapWritable dividedByScala(MapWritable vector, double scala){
		MapWritable ret = new MapWritable();
		Set<Entry<Writable, Writable>> keyVals = vector.entrySet();
		Iterator<Entry<Writable, Writable>> iter = keyVals.iterator();
		while(iter.hasNext()){
			Entry<Writable, Writable> entry = iter.next();
			IntWritable key = (IntWritable) entry.getKey();
			double val = ((DoubleWritable) entry.getValue()).get() / scala;
			ret.put(key, new DoubleWritable(val));
		}

		return ret;
	}
	
	//稀疏向量除以标量
	public static double[] dividedByScala(double[] vector, double scala){
		double[] ret = new double[vector.length];
		for(int i = 0; i < vector.length; i++)
			ret[i] = vector[i]/scala;
		return ret;
	}
	
	//计算欧式距离
	public static double getEuclideanDistance(double[] vector1, double[] vector2){
		if(vector1.length != vector2.length)
			throw new IllegalArgumentException("two vector's size not equal!");
		double ret = 0;
		for(int i = 0; i < vector1.length; i++){
			ret += (vector1[i] - vector2[i]) * (vector1[i] - vector2[i]);
		}
		return Math.sqrt(ret);
	}
	
	//稀疏向量计算欧式距离
	public static double getEuclideanDistance(MapWritable vector1, MapWritable vector2){
		Set<Writable> keySet = new HashSet<>(vector1.keySet());
		Set<Writable> keySet1 = vector1.keySet();
		Set<Writable> keySet2 = vector2.keySet();
		keySet.addAll(keySet2);
		
		Iterator<Writable> iter =  keySet.iterator();
		double val1, val2;
		double ret = 0;
		while(iter.hasNext()){
			IntWritable ix = (IntWritable) iter.next();
			val1 = val2 = 0;
			if(keySet1.contains(ix))
				val1 = ((DoubleWritable) vector1.get(ix)).get();
			if(keySet2.contains(ix))
				val2 = ((DoubleWritable) vector2.get(ix)).get();
			ret += (val1 - val2) * (val1 - val2);
		}
		
		return Math.sqrt(ret);
	}
}
