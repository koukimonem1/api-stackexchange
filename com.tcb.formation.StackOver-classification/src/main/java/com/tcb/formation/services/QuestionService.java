package com.tcb.formation.services;

import com.tcb.formation.storage.Question;

public interface QuestionService {
	public Question getQuestion(Long id, int label);
}
