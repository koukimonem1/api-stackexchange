package com.tcb.formation;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.PropertySource;

import com.tcb.formation.storage.hbase.HbaseDao;
import com.tcb.formation.storage.hive.HiveDAO;

@Configuration
@ComponentScan
@PropertySource("classpath:application.properties")
public class ApplicationConfig {

	@Value("${warehouse.database}")
	String dbWarehouse;

	@Bean
	public SparkSession getSparkSession() {
		return SparkSession.builder().appName("stack overflow classification")
				.config("spark.sql.warehouse.dir", dbWarehouse)
				.enableHiveSupport().master("local[*]").getOrCreate();
	}

	@Bean
	@DependsOn("getSparkSession")
	public SparkContext getHbaseConfiguration() {
		getSparkSession().conf().set("hbase.zookeeper.quorum", "127.0.1.1");
		getSparkSession().conf().set("hbase.zookeeper.property.clientPort",
				"2181");
		getSparkSession().conf().set("zookeeper.znode.parent",
				"/hbase-unsecure");
		return getSparkSession().sparkContext();
	}

	@Bean("hiveDAO")
	public HiveDAO getHiveDao() {
		return new HiveDAO();
	}
	
	@Bean("hbaseDAO")
	public HbaseDao getHbaseDao() {
		return new HbaseDao();
	}
}
