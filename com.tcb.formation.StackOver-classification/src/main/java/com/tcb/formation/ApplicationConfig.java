package com.tcb.formation;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.PropertySource;

import com.tcb.formation.storage.hbase.HbaseDAO;
import com.tcb.formation.storage.hive.HiveDAO;

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
				.enableHiveSupport().master("local[*]")
				.getOrCreate();
	}

	@Bean
	@DependsOn("getSparkSession")
	public Job getJob() throws IOException {
		Job job = Job.getInstance(getSparkSession().sparkContext()
				.hadoopConfiguration());
		job.getConfiguration().set("hbase.zookeeper.quorum", "127.0.1.1");
		job.getConfiguration().set("hbase.zookeeper.property.clientPort",
				"2181");
		job.getConfiguration().set("zookeeper.znode.parent", "/hbase-unsecure");
		job.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
		return job;
	}

	@Bean
	public org.apache.hadoop.conf.Configuration getConfiguration() {
		org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "127.0.1.1");
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		return conf;
	}

	@Bean("hiveDAO")
	public HiveDAO getHiveDao() {
		return new HiveDAO();
	}

	@Bean("hbaseDAO")
	public HbaseDAO getHbaseDao() {
		return new HbaseDAO();
	}
}
