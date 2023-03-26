package com.dhpark.datagen.service;

import com.dhpark.datagen.generator.DataGenerator;
import com.dhpark.datagen.producer.DataProducer;

public class GenerateService implements Runnable {
	private String topicName;
	
	
	public GenerateService(String topicName) {
		this.topicName = topicName;
	}


	@Override
	public void run() {
		DataGenerator generator = new DataGenerator(topicName);
		DataProducer producer = new DataProducer(topicName);
		int index = 1;
		// 10000건 생성하여 Topic에 Producing
		for(; index <= 10000; index++) {
			producer.produceData(topicName, generator.generateData(), index);
		}
		System.out.println((index -1) + " record Producing Success! = " + topicName);
		producer.closeProducer();
	}
}
