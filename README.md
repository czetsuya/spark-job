# Spark Job Demo

Compiled in run on JDK 8.

How to run Spark
-

## Example 1: The famous WordCounter.

>spark-submit --class com.broodcamp.spark.WordCounter --master local target/spark-job-0.0.1-SNAPSHOT.jar src/main/resources/input.txt

*Now let's try to run Spark by processing data from https://grouplens.org/datasets/movielens/100k/.

## Example 2: Count the movies rated by n. For example rating 3 has 100 movies, etc.

UserId MovieId Rating TimeStamp

196	242	3	881250949

>spark-submit --class com.broodcamp.spark.RatingsAverage --master local target/spark-job-0.0.1-SNAPSHOT.jar src/main/resources/u.data