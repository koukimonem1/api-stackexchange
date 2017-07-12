package com.tcb.formation.services;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import scala.collection.JavaConversions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.tcb.formation.storage.DictionaryWord;
import com.tcb.formation.storage.OperationDAO;
import com.tcb.formation.storage.OperationDAOFactory;
import com.tcb.formation.storage.Question;
import com.tcb.formation.storage.StopWord;
import com.tcb.formation.util.ListAccumulator;

@Component
@Scope("singleton")
public class QuestionServiceImpl implements QuestionService {
	@Autowired
	private OperationDAOFactory factory;
	private Stemmer stemmer = new Stemmer();
	private List<DictionaryWord> bow;
	private ListAccumulator<StopWord> newStopWords = new ListAccumulator<StopWord>();
	private ListAccumulator<DictionaryWord> newWords = new ListAccumulator<DictionaryWord>();
	@Value("${stackexchange.question}")
	private String urlQuestion;
	@Value("${stackexchange.question.body}")
	private String questionBody;

	public Question getQuestion(Long id, int label,String type) {
		Question question = null;
		List<String> tags = new ArrayList<String>();
		String body = "";
		OperationDAO dao = factory.getOperationDao(type);
		String urlString = urlQuestion + id + questionBody;
		List<StopWord> listSW = new ArrayList<StopWord>(dao.getStopWords());
		ObjectMapper mapper = new ObjectMapper();
		try {
			URL url = new URL(urlString);
			URLConnection con = url.openConnection();
			InputStream in = con.getInputStream();
			GZIPInputStream ginput = new GZIPInputStream(in);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
			BufferedReader br = new BufferedReader(
					new InputStreamReader(ginput));
			String jsonString;
			jsonString = br.readLine();
			JsonNode jsonNode = mapper.readTree(jsonString);
			JsonNode itemsJson = mapper.readTree(jsonNode.get("items")
					.toString());
			JsonNode tagsJson;
			if (itemsJson.isArray()) {
				for (final JsonNode objNode : itemsJson) {
					id = Long.parseLong(objNode.get("question_id").toString());
					body = objNode.get("title").toString() + " "
							+ objNode.get("body").toString();
					tagsJson = objNode.get("tags");
					body = body.replaceAll("<a .*?/>", " ")
							.replaceAll("<code>.*?</code>", " ")
							.replaceAll("<.*?>", " ")
							.replaceAll("\\\\n", " ")
							.replaceAll("\"", " ")
							.replaceAll("/", " ")
							.replaceAll("&", " ")
							.replaceAll("\\\\", " ")
							.replaceAll("#", " ")
							.replaceAll("-", " ")
							.replaceAll("['=()?:!.,;{}*0-9]+", " ") 
							.replaceAll("\\[", " ")
							.replaceAll("\\]", " ")
							.toLowerCase();
					if (tagsJson.isArray()) {
						for (final JsonNode tagNode : tagsJson) {
							tags.add(tagNode.textValue());
						}
					}
				}
			}

			/**************************************************
			 * All tags are considered as stop words
			 * ************************************************/

			Iterator<String> itr = tags.iterator();
			while (itr.hasNext()) {
				String word = itr.next();
				if (!listSW.contains(word)) {
					StopWord sw = new StopWord(word);
					newStopWords.add(new ArrayList<StopWord>(Collections
							.singleton(sw)));
					listSW.add(sw);
				}
			}
			dao.saveStopWords(JavaConversions.asScalaBuffer(
					newStopWords.value()).seq());

			/******************************************************
			 * Remove all stop words
			 * ****************************************************/

			List<String> words = new ArrayList<String>(Arrays.asList(body
					.split(" +")));
			Iterator<String> itrWords = words.iterator();
			while (itrWords.hasNext()) {
				String sw = itrWords.next();
				StopWord word = new StopWord(sw);
				if (listSW.contains(word)) {
					itrWords.remove();
				}
			}

			/**************************************************************************************
			 * Stemming + save words that does not exist in the dictionary when we are using hive
			 * ************************************************************************************/

			bow = new ArrayList<DictionaryWord>(dao.getBagOfWords());
			List<String> stemmedBody = new ArrayList<String>(Lists.transform(
					words, stemmingBody));
			Iterator<String> stemmedIterator = stemmedBody.iterator();
			while (stemmedIterator.hasNext()) {
				DictionaryWord word = new DictionaryWord(stemmedIterator.next());
				if ((!bow.contains(word) && type.equals("hive"))
						|| type.equals("hbase")) {
					newWords.add(new ArrayList<DictionaryWord>(Collections
							.singleton(word)));
					bow.add(word);
				}
			}
			dao.saveWords(JavaConversions.asScalaBuffer(newWords.value()).seq());
			/****************************************************
			 * Create question object
			 ****************************************************/

			question = new Question(id, JavaConversions.asScalaBuffer(
					stemmedBody).seq(), JavaConversions.asScalaBuffer(tags)
					.seq(), label);

			/****************************************************/

			conn.disconnect();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return question;
	}

	/******************************************************
	 * Function that stems all string elements in a list
	 * ****************************************************/

	private Function<String, String> stemmingBody = new Function<String, String>() {
		public String apply(String word) {
			return stemmer.stem(word);
		}
	};

	public OperationDAOFactory getFactory() {
		return factory;
	}

	public void setFactory(OperationDAOFactory factory) {
		this.factory = factory;
	}

}
