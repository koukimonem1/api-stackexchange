package com.tcb.formation.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ResourceBundle;

import org.apache.spark.ml.feature.StopWordsRemover;

public class JavaCreateDatabae {

	private static String driverName = Constants.getConstants("jdbc.driver");
	private static String jdbcURL = Constants.getConstants("jdbc.url");

	private JavaCreateDatabae() {
	}

	public static void execute() {
		try {
			Class.forName(driverName);
			Connection con = DriverManager.getConnection(jdbcURL, "", "");
			Statement stmt = con.createStatement();
			stmt.execute("CREATE TABLE IF NOT EXISTS question( id INT,"
					+ " body ARRAY<STRING>,  tags ARRAY<STRING>,"
					+ " label INT) CLUSTERED BY(label) INTO 3 BUCKETS");
			stmt.execute("CREATE TABLE IF NOT EXISTS Dictionnaire(word STRING)");
			stmt.execute("CREATE TABLE IF NOT EXISTS stopwords(stopword STRING)");

			/****************************************************************
			 * Create this table just to insert arrays in collection columns
			 * **************************************************************/
			stmt.execute("create table if not exists dummy(a string)");
			// stmt.execute("insert into table dummy values ('a')");

			StopWordsRemover remover = new StopWordsRemover();
			String[] stopWords = remover.getStopWords();
			System.out.println("create default stop words");
			for (int i = 0; i < stopWords.length; i++) {
				if (!stopWords[i].equals("how"))
					stmt.execute("INSERT INTO stopwords (stopword) VALUES ('"
							+ stopWords[i] + "')");
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public String getConstants(String key) {
		ResourceBundle bundle = ResourceBundle.getBundle("application");
		String value = bundle.getString(key);
		return value;
	}
}
