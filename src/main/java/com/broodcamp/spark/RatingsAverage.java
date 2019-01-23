package com.broodcamp.spark;

import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Calculate the average rating given by each user.
 * <p>
 * Sample input: 196 242 3 881250949
 * </p>
 * 
 * @author Edward P. Legaspi
 */
public class RatingsAverage {

	private static final Pattern TAB = Pattern.compile("\\t");

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.err.println("Usage: RatingsAverage <file>");
			System.exit(1);
		}
		SparkConf sparkConf = new SparkConf().setAppName("RatingsAverage").setMaster("local[4]");
		try (JavaSparkContext ctx = new JavaSparkContext(sparkConf)) {
			JavaPairRDD<Object, Object> ratings = ctx.textFile(args[0], 4) //
					// (196, 3) -> JavaPairRDD<Object, Object>
					.mapToPair(s -> new Tuple2<>(Integer.parseInt(TAB.split(s)[0]), Integer.parseInt(TAB.split(s)[2]))) //
					// (196, (rating, count)) -> JavaPairRDD<Integer, Object>
					.mapValues(t -> new Tuple2<>(t, 1)) //
					// (196, (ratingsSum, countSum))
					// -> JavaPairRDD<Integer, Tuple2<Integer, Integer>>
					.reduceByKey((a, c) -> new Tuple2<>(a._1 + c._1, a._2 + c._2)) //
					// Remember we have (196, (ratingsSum, countSum))
					// In x._2._1 ._2 means second tuple ._1 index 1
					// As our first tuple is (192, (x, y).
					.mapToPair(x -> new Tuple2<>(x._1, (double) x._2._1 / x._2._2)) //
			;

			for (Tuple2<?, ?> tuple : ratings.collect()) {
				System.out.println(tuple._1() + "=" + tuple._2());
			}

			ctx.stop();
		}
	}
}
