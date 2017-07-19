package com.tcb.formation.services;

import java.util.List;

import com.tcb.formation.storage.Question;

public interface QuestionService {

	public Question getQuestion(Long id, int label, String type);

	public List<Integer> getStreamingIds();

}
