package com.broodcamp.spark;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Get the worst rated movies (rating = 1).
 * 
 * <pre>
 * Sample input: 196 242 3 881250949
 * userId	movieId	rating	time
 * </pre>
 * 
 * As for the u.item
 * 
 * <pre>
 * 1|Toy Story
 * movieId|movieTitle
 * </pre>
 * 
 * @author Edward P. Legaspi
 */
public class WorstMovies {
	private static final Pattern TAB = Pattern.compile("\\t");

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: WorstMovies <file> <file>");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("WorstMovies").setMaster("local[4]");
		try (JavaSparkContext ctx = new JavaSparkContext(sparkConf)) {

			// Lookup

			JavaRDD<String> namesLookup = ctx.textFile(args[1], 4);
			// (movieId, movieTitle)
			JavaPairRDD<Integer, String> namesLookupPairRDD = namesLookup.mapToPair(p -> new Tuple2<>(Integer.parseInt(p.split("\\|")[0]), p.split("\\|")[1]));

			// Ratings

			List<Tuple2<Integer, Double>> ratingsTotalAndCount = ctx.textFile(args[0], 4) //
					// (movieId, rating)
					.mapToPair(p -> new Tuple2<>(Integer.parseInt(TAB.split(p)[1]), Double.parseDouble(TAB.split(p)[2])))
					// (movieId, (rating, count))
					.mapValues(p -> new Tuple2<>(p, 1.0)) //
					// (movieId, (ratingsSum, countSum))
					// -> JavaPairRDD<Integer, Tuple2<Integer, Double>>
					.reduceByKey((a, c) -> new Tuple2<>(a._1 + c._1, a._2 + c._2)) //
					// Remember we have (movieId, (ratingsSum, countSum))
					// In x._2._1 ._2 means second tuple ._1 index 1
					// (x, y)
					.mapToPair(x -> new Tuple2<>(x._1, (double) x._2._1 / x._2._2)) //
					// need to sort by y
					.filter(p -> p._2 < 2) //
					// flip the Tuple as Spark does not support sort by value
					.mapToPair(a -> new Tuple2<Double, Integer>(a._2, a._1)) //
					.sortByKey(true) //
					// flip again, actually this step is optional you can simply adjust the loop
					// index below
					.mapToPair(b -> new Tuple2<Integer, Double>(b._2, b._1)) //
					.take(10) //
			;

			System.out.println("-----------------------------------------------START");
			List<Tuple2<Integer, String>> namesLookupPair = namesLookupPairRDD.collect();

			for (Tuple2<?, ?> tuple : ratingsTotalAndCount.toArray(new Tuple2[0])) {
				System.out.println(namesLookupPair.stream().filter(p -> p._1.equals(tuple._1)).findFirst().get()._2 + " : " + tuple._2);
			}
			System.out.println("-----------------------------------------------END");

			ctx.stop();
		}
	}
}
