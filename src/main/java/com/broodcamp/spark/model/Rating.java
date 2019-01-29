package com.broodcamp.spark.model;

/**
 * @author Edward P. Legaspi
 */
public class Rating {

	private int userId;
	private int movieId;
	private float rating;
	private long timestamp;

	public Rating() {
	}

	public Rating(int userId, int movieId, float rating, long timestamp) {
		this.userId = userId;
		this.movieId = movieId;
		this.rating = rating;
		this.timestamp = timestamp;
	}

	public static Rating parseRating(String str) {
		String[] fields = str.split("::");
		if (fields.length != 4) {
			throw new IllegalArgumentException("Each line must contain 4 fields");
		}
		int userId = Integer.parseInt(fields[0]);
		int movieId = Integer.parseInt(fields[1]);
		float rating = Float.parseFloat(fields[2]);
		long timestamp = Long.parseLong(fields[3]);
		return new Rating(userId, movieId, rating, timestamp);
	}

	public int getUserId() {
		return userId;
	}

	public void setUserId(int userId) {
		this.userId = userId;
	}

	public int getMovieId() {
		return movieId;
	}

	public void setMovieId(int movieId) {
		this.movieId = movieId;
	}

	public float getRating() {
		return rating;
	}

	public void setRating(float rating) {
		this.rating = rating;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

}
