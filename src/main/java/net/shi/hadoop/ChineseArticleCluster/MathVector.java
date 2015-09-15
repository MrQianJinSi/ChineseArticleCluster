package net.shi.hadoop.ChineseArticleCluster;

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
	
	//向量除以标量
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
}
