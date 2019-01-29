package com.broodcamp.spark;

import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.broodcamp.spark.model.Rating;

import scala.Tuple2;

/**
 * Create a user 0 who logs science fiction but hates drama.
 * 
 * @author Edward P. Legaspi
 */
public class MovieRecommendationsALS2 {

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

			JavaRDD<Rating> ratingsRDD = spark.read().textFile(args[0]).javaRDD().map(Rating::parseRating);
			Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, Rating.class);
			Dataset<Row>[] splits = ratings.randomSplit(new double[] { 0.8, 0.2 });
			Dataset<Row> training = splits[0];
			Dataset<Row> test = splits[1];

			ALS als = new ALS();
			als.setMaxIter(5);
			als.setRegParam(0.01);
			als.setUserCol("userId");
			als.setItemCol("movieId");
			als.setRatingCol("rating");

			ALSModel model = als.fit(training);

			// Evaluate the model by computing the RMSE on the test data
			// Note we set cold start strategy to 'drop' to ensure we don't get NaN
			// evaluation metrics
			model.setColdStartStrategy("drop");

			Dataset<Row> predictions = model.transform(test);

			RegressionEvaluator evaluator = new RegressionEvaluator() //
					.setMetricName("rmse") //
					.setLabelCol("rating") //
					.setPredictionCol("prediction") //
			;

			Double rmse = evaluator.evaluate(predictions);
			System.out.println("Root-mean-square error = " + rmse);

			// Generate top 10 movie recommendations for each user
			Dataset<Row> userRecs = model.recommendForAllUsers(10);

			// Generate top 10 user recommendations for each movie
			Dataset<Row> movieRecs = model.recommendForAllItems(10);

			// Generate top 10 movie recommendations for a specified set of users
			Dataset<Row> users = ratings.select(als.getUserCol()).distinct().limit(3);
			Dataset<Row> userSubsetRecs = model.recommendForUserSubset(users, 10);

			// Generate top 10 user recommendations for a specified set of movies
			Dataset<Row> movies = ratings.select(als.getItemCol()).distinct().limit(3);
			Dataset<Row> movieSubSetRecs = model.recommendForItemSubset(movies, 10);

			// print
			userRecs.show();
			movieRecs.show();
			userSubsetRecs.show();
			movieSubSetRecs.show();

			spark.stop();
		}
		System.out.println("--------------------------------------FINISHED");
		Date stopTime = new Date();
		long diff = stopTime.getTime() - startTime.getTime();

		System.out.println("Time(s)=" + diff / 1000);
	}

}
