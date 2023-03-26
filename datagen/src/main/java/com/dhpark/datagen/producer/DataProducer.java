package com.dhpark.datagen.producer;

import java.io.ByteArrayOutputStream;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;

public class DataProducer {
	
	private KafkaProducer<String, byte[]> producer;
	
	public DataProducer(String topicName) {
		this.producer = createProducer(topicName);
	}
	
	private KafkaProducer<String, byte[]> createProducer(String topicName){
		Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.165:9092,192.168.0.165:9093,192.168.0.165:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        // exactly-once를 위한 config (멱등성)
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // exactly-once를 위한 config (Transaction의 원자성 실행)
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, topicName + "-transactional-id");
        KafkaProducer<String, byte[]> kafkaProducer = new KafkaProducer<>(props);
        kafkaProducer.initTransactions();
        System.out.println("Producer Create Success! = " + topicName);
        return kafkaProducer;
	}
	
	public void produceData(String topicName, ByteArrayOutputStream data, int index) {
		ProducerRecord<String, byte[]> record = new ProducerRecord<>(topicName, topicName + "-" + index ,data.toByteArray());
		// 1000건씩 flush하기 위함 
		try {
			if(index % 1000 == 1) {
				producer.beginTransaction();
			}
			producer.send(record);
			if(index % 1000 == 0) {
				producer.flush();
				producer.commitTransaction();
			}
		}catch(Exception e) {
			e.printStackTrace();
			System.out.println("Transaction Fail! index = " + index);
			producer.abortTransaction();
		}
	}
	
	public void closeProducer() {
		producer.flush();
		try {
			producer.commitTransaction();			
		}catch(KafkaException e) {
			System.out.println("This operation is divided by 1000!");
		}
		producer.close();
	}
}
