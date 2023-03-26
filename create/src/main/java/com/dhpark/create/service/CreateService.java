package com.dhpark.create.service;

import java.util.concurrent.CountDownLatch;

import com.dhpark.create.creator.SchemaCreator;
import com.dhpark.create.creator.TableCreator;
import com.dhpark.create.creator.TopicCreator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CreateService {
public void create(String jsonStr, String type) {
		
		ObjectMapper objectMapper = new ObjectMapper();
		JsonNode schemaNode = null;
		try {
			schemaNode = objectMapper.readTree(jsonStr);
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		
		// 순차 진행을 위해 CountDownLatch 사용 
		CountDownLatch datasetCount = new CountDownLatch(schemaNode.size());
		
		for (JsonNode dataset : schemaNode) {
			//병렬 처리를 위해 Thread 사용
			Runnable creator = null;
			// 타입에 맞는 Creator 객체 생성 
			switch(type) {
				case "schema":{
					creator = new SchemaCreator(objectMapper.convertValue(dataset, JsonNode.class), datasetCount);
					break;
				}
				case "topic":{
					creator = new TopicCreator(objectMapper.convertValue(dataset, JsonNode.class), datasetCount);
					break;
				}
				case "table":{
					creator = new TableCreator(objectMapper.convertValue(dataset, JsonNode.class), datasetCount);
					break;
				}
			}
			Thread schemaConvertThread = new Thread(creator);
			schemaConvertThread.start();
		}
		
		// Thread에서 작업이 전부 끝날 때 까지 대기
		try {
			datasetCount.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
