package com.broodcamp.spark.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * @author Edward P. Legaspi
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
@EqualsAndHashCode
public class User {

	private int user_id;
	private int age;
	private String gender;
	private String occupation;
	private String zip;

	public static User parse(String line) {
		String[] fields = line.split("\\|");
		if (fields.length != 5) {
			throw new IllegalArgumentException("Each line must contain 5 fields");
		}
		int userId = Integer.parseInt(fields[0]);
		int age = Integer.parseInt(fields[1]);
		String gender = fields[2];
		String occupation = fields[3];
		String zip = fields[4];

		return new User(userId, age, gender, occupation, zip);
	}

}
