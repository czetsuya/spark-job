package com.broodcamp.spark;

import static org.apache.spark.sql.functions.lit;

import java.util.Date;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import scala.Tuple2;

/**
 * Create a user 0 who loves science fiction but hates drama.
 * 
 * @author Edward P. Legaspi
 */
public class MovieRecommendationsALS {

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: MovieRecommendationsALS <file> <file>");
			System.exit(1);
		}

		Date startTime = new Date();
		System.out.println("--------------------------------------START");
		SparkConf sparkConf = new SparkConf().setAppName("MovieRecommendationsALS");
		sparkConf.set("spark.sql.crossJoin.enabled", "true");
		try (SparkSession spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()) {
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

			Dataset<Row> ratings = movieRawDataset //
					.withColumn("userId", movieRawDataset.col("userId").cast(DataTypes.IntegerType)) //
					.withColumn("movieId", movieRawDataset.col("movieId").cast(DataTypes.IntegerType)) //
					.withColumn("rating", movieRawDataset.col("rating").cast(DataTypes.DoubleType)) //
					.cache() //
			;

			ALS als = new ALS();
			als.setMaxIter(5);
			als.setRegParam(0.01);
			als.setUserCol("userId");
			als.setItemCol("movieId");
			als.setRatingCol("rating");

			ALSModel model = als.fit(ratings);

			System.out.println("Ratings for user with id=0");
			System.out.println("--");

			Dataset<Row> user0Ratings = ratings.filter("userId = 0");

			for (Row rating : user0Ratings.collectAsList()) {
				String movieName = namesLookupPairRDD.filter(p -> p._1 == Integer.parseInt(rating.getAs("movieId").toString())).first()._2;
				System.out.println(movieName + "," + rating.getAs("rating"));
			}

			// process only movies that are rated more than 100x
			Dataset<Row> ratingCounts = ratings.groupBy("movieId").count().filter("count > 100");

			// map the movies rated with > 100x to our user with id 0 for testing
			Dataset<Row> popularMovies = ratingCounts.select("movieId").withColumn("userId", lit(0));

			// run the model on the list of popular movies
			Dataset<Row> recommendations = model.transform(popularMovies);

			recommendations.show();

			List<Row> topRecommendations = recommendations.sort(new Column("prediction").desc()).takeAsList(20);

			for (Row recommendation : topRecommendations) {
				String movieName = namesLookupPairRDD.filter(p -> p._1 == Integer.parseInt(recommendation.getAs("movieId").toString())).first()._2;
				System.out.println(movieName + "," + recommendation.getAs("prediction"));
			}

			spark.stop();
		}
		System.out.println("--------------------------------------FINISHED");
		Date stopTime = new Date();
		long diff = stopTime.getTime() - startTime.getTime();

		System.out.println("Time(s)=" + diff / 1000);
	}

}
