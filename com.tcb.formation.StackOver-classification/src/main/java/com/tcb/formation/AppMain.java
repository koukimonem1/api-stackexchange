package com.tcb.formation;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.tcb.formation.services.QuestionService;
import com.tcb.formation.storage.OperationDAO;
import com.tcb.formation.storage.Question;

public class AppMain {

	public static void main(String[] args) {
		/**
		 * The main method should take four parameters
		 *  ++++ type : indicates the type of storage layer, it should be equal to 'hive' or 'hbase'
		 *  ++++ indice : should be equal 1 if the database isn't created yet
		 *  ++++ idQuestion : the id question
		 *  ++++ label : the class of that question
		 */
		String type = args[0];
		int indice = Integer.parseInt(args[1]);
		long idQuestion = Long.parseLong(args[2]);
		int label = Integer.parseInt(args[3]);
		@SuppressWarnings("resource")
		ApplicationContext context = new AnnotationConfigApplicationContext(
				ApplicationConfig.class);

		OperationDAO dao = context.getBean(type + "DAO", OperationDAO.class);
		if (indice == 1)
			dao.createDatabase();
		QuestionService service = context.getBean(QuestionService.class);
		Question question = service.getQuestion(idQuestion, label, type);
		dao.saveQuestion(question);
	}

}
