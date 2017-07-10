package com.tcb.formation;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.tcb.formation.services.QuestionService;
import com.tcb.formation.storage.OperationDAO;
import com.tcb.formation.storage.Question;

public class HbaseMain {
	public static void main(String[] args) {
		@SuppressWarnings("resource")
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(
				ApplicationConfig.class);
		OperationDAO dao  = context.getBean("hbaseDAO",OperationDAO.class);
		QuestionService service = context.getBean(QuestionService.class);
		/**
		 * The main method should take three parameters
		 *  ++++ indice : should be equal 1 if the database isn't created yet
		 *  ++++ idQuestion : the id question
		 *  ++++ label : the class of that question
		 */
		int indice = 0;// Integer.parseInt(args[0]);
//		long idQuestion = Long.parseLong(args[1]);
//		int label = Integer.parseInt(args[2]);
		if (indice == 1)
			dao.createDatabase();		
		Question question = service.getQuestion(22997137L, 1, "hbase");
	 	dao.saveQuestion(question);
	}
}
