package com.tcb.formation;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@ComponentScan
@PropertySource("classpath:application.properties")
public class ApplicationConfig {
	
	@Value("${warehouse.database}")
	String dbWarehouse;

	@Bean
	public SparkSession getSparkSession() {
		return SparkSession
				.builder()
				.appName("stack overflow classification")
				.config("spark.sql.warehouse.dir", dbWarehouse)
				.enableHiveSupport()
				.master("local[*]")
				.getOrCreate();
	}
}
