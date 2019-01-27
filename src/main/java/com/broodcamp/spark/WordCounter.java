package com.broodcamp.spark;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Let's count the words.
 * 
 * @author Edward P. Legaspi
 */
public class WordCounter {

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {
		
		if (args.length < 1) {
			System.err.println("Usage: WordCounter <file>");
			System.exit(1);
		}
		
		SparkConf sparkConf = new SparkConf().setAppName("WordCounter").setMaster("local[4]");
		try (JavaSparkContext ctx = new JavaSparkContext(sparkConf)) {
			JavaPairRDD<String, Integer> wordStats = ctx.textFile(args[0], 4) //
					.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator()) //
					.mapToPair(word -> new Tuple2<>(word, 1)) //
					.reduceByKey((v1, v2) -> v1 + v2);

			for (Tuple2<?, ?> tuple : wordStats.collect()) {
				System.out.println(tuple._1() + "=" + tuple._2());
			}

			// wordStats.saveAsTextFile("");

			ctx.stop();
		}
	}
}
