package com.broodcamp.spark;

import java.util.Date;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

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
public class WorstMoviesV2 {

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: WorstMoviesV2 <file> <file>");
			System.exit(1);
		}

		Date startTime = new Date();
		System.out.println("--------------------------------------START");
		try (SparkSession spark = SparkSession.builder().enableHiveSupport().appName("WorstMoviesV2").getOrCreate()) {
			// lookup
			JavaRDD<String> namesLookup = spark.sparkContext().textFile(args[1], 4).toJavaRDD();
			// (movieId, movieTitle)
			JavaPairRDD<Integer, String> namesLookupPairRDD = namesLookup.mapToPair(p -> new Tuple2<>(Integer.parseInt(p.split("\\|")[0]), p.split("\\|")[1]));

			Dataset<Row> movieRawDataset = spark.read() //
					.option("header", "false") //
					.option("sep", "\t") //
					.csv(args[0]) //
					.toDF("userId", "movieId", "rating", "time") //
			;

			Dataset<Row> movieDataset = movieRawDataset.withColumn("rating", movieRawDataset.col("rating").cast(DataTypes.IntegerType));

			Dataset<Row> averageRatings = movieDataset.select("movieId", "rating") //
					.groupBy("movieId") //
					.avg("rating") //
					.withColumnRenamed("avg(rating)", "rating") //
			;

			Dataset<Row> counts = movieDataset.groupBy("movieId") //
					.count() //
			;

			Dataset<Row> averageAndAcounts = counts.join(averageRatings, "movieId");

			List<Row> topTen = averageAndAcounts.orderBy("rating").takeAsList(10);

			topTen.stream().forEach(m -> {
				String movieName = namesLookupPairRDD.filter(p -> p._1 == Integer.parseInt(m.get(0).toString())).first()._2;
				System.out.println(movieName + "," + m.get(1) + "," + m.get(2));
			});

			spark.stop();
		}
		System.out.println("--------------------------------------FINISHED");
		Date stopTime = new Date();
		long diff = stopTime.getTime() - startTime.getTime();

		System.out.println("Time(s)=" + diff / 1000);
	}
}
