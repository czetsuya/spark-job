# Spark Job Demo

Compiled in run on JDK 8.

How to run Spark
-

## Example 1: The famous WordCounter

>spark-submit --class com.broodcamp.spark.WordCounter --master local target/spark-job-0.0.1-SNAPSHOT.jar src/main/resources/input.txt

*Now let's try to run Spark by processing data from https://grouplens.org/datasets/movielens/100k/.

## Example 2: Compute the average rating given by a user

UserId MovieId Rating TimeStamp

196	242	3	881250949

>spark-submit --class com.broodcamp.spark.RatingsAverage --master local target/spark-job-0.0.1-SNAPSHOT.jar src/main/resources/u.data

### Example 3: Old five-star movies

### Example 4: Most rated one-star movies

>spark-submit --class com.broodcamp.spark.WorstMovies --master local target/spark-job-0.0.1-SNAPSHOT.jar src/main/resources/u.data src/main/resources/u.item

>spark-submit --class com.broodcamp.spark.WorstMoviesV2 --master local target/spark-job-0.0.1-SNAPSHOT.jar src/main/resources/u.data src/main/resources/u.item

### Example 5: Get the square of each element in the set

>spark-submit --class com.broodcamp.spark.NumberSquare --master local target/spark-job-0.0.1-SNAPSHOT.jar

### Example 6. Movie recommendation

>spark-submit --class com.broodcamp.spark.MovieRecommendationsALS --master local target/spark-job-0.0.1-SNAPSHOT.jar src/main/resources/u-ml.data src/main/resources/u.item

>spark-submit --class com.broodcamp.spark.MovieRecommendationsALS2 --master local target/spark-job-0.0.1-SNAPSHOT.jar src/main/resources/sample_movielens_ratings.txt src/main/resources/u.item