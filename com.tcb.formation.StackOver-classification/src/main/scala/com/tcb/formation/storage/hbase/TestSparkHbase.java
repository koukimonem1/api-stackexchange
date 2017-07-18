package com.tcb.formation.storage.hbase;

import java.util.List;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.tcb.formation.ApplicationConfig;

public class TestSparkHbase {
	public static void main(String[] args) {
		@SuppressWarnings("resource")
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(
				ApplicationConfig.class);
		HbaseDAO dao = context.getBean(HbaseDAO.class);
		List<String> list = dao.filterBagOfWords();
		for (String s : list) {
			System.out.println(s);
		}
	}
}
