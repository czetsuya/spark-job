package com.broodcamp.spark;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.broodcamp.spark.model.User;

/**
 * Insert data to Cassandra database.
 * <p>
 * This class is using Lombok plugin.
 * </p>
 * Create the keyspace and table.
 * 
 * <pre>
 * create keyspace movielens with replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' } and durable_writes=true;
 * use movielens;
 * create table users (user_id int, age int, gender text, occupation text, zip text, primary key (user_id));
 * </pre>
 * 
 * @author Edward P. Legaspi
 */
public class MongoIntegration {

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: MongoIntegration <file>");
			System.exit(1);
		}

		Date startTime = new Date();
		System.out.println("--------------------------------------START");
		SparkConf sparkConf = new SparkConf().setAppName("MongoIntegration");
		sparkConf.set("spark.sql.crossJoin.enabled", "true");

		try (SparkSession spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()) {
			JavaRDD<User> usersRDD = spark.read().textFile(args[0]).javaRDD().map(User::parse);
			Dataset<Row> users = spark.createDataFrame(usersRDD, User.class);

			Map<String, String> options = new HashMap<>();
			options.put("uri", "mongodb://127.0.0.1/movielens.users");

			users.write() //
					.format("com.mongodb.spark.sql.DefaultSource") //
					.options(options) //
					.mode("append") //
					.save();

			Dataset<Row> loadedUsers = spark.read() //
					.format("com.mongodb.spark.sql.DefaultSource") //
					.options(options) //
					.load() //
			;

			loadedUsers.createOrReplaceTempView("users");

			Dataset<Row> sqlDF = spark.sql("SELECT * FROM users WHERE age < 20");
			sqlDF.show();

			spark.stop();
		}
		System.out.println("--------------------------------------FINISHED");
		Date stopTime = new Date();
		long diff = stopTime.getTime() - startTime.getTime();

		System.out.println("Time(s)=" + diff / 1000);
	}

}
