package com.broodcamp.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Create an RDD and square its contents.
 * 
 * @author Edward P. Legaspi
 */
public class NumberSquare {

	public static void main(String[] args) throws Exception {

		SparkConf sparkConf = new SparkConf().setAppName("NumberSquare").setMaster("local[4]");
		try (JavaSparkContext ctx = new JavaSparkContext(sparkConf)) {
			JavaRDD<Integer> nums = ctx.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7));

			nums.map(n -> n * n).collect().forEach(System.out::println);

			ctx.stop();
		}
	}

}
