package com.tcb.formation;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.tcb.formation.services.QuestionService;
import com.tcb.formation.storage.hive.HiveDAO;
import com.tcb.formation.storage.Question;

public class Main {

	public static void main(String[] args) {
		@SuppressWarnings("resource")
		ApplicationContext context = new AnnotationConfigApplicationContext(
				ApplicationConfig.class);
		/**
		 * The main method should take three parameters
		 *  ++++ indice : should be equal 1 if the database isn't created yet
		 *  ++++ idQuestion : the id question
		 *  ++++ label : the class of that question
		 */
		int indice = Integer.parseInt(args[0]);
		long idQuestion = Long.parseLong(args[1]);
		int label = Integer.parseInt(args[2]);
		
		HiveDAO dao = context.getBean(HiveDAO.class);
		if (indice == 1)
			dao.createDatabase();
		QuestionService service = context.getBean(QuestionService.class);
		Question question = service.getQuestion(idQuestion, label);
		dao.saveQuestion(question);
	}

}
