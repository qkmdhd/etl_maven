package com.dhpark.datatransfer.consumer;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class DataConsumer {
	private KafkaConsumer<String, byte[]> consumer;
	private Schema schema;
	
	public DataConsumer(String topicName) {
		this.consumer = createConsumer(topicName);
		this.schema = getSchemaFromFile(topicName);
	}
	
	private KafkaConsumer<String, byte[]> createConsumer(String topicName) {
		Properties props = new Properties();
	    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.165:9092,192.168.0.165:9093,192.168.0.165:9094");
	    props.put(ConsumerConfig.GROUP_ID_CONFIG, topicName + "-consumer-group");
	    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
	    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
	    // 비즈니스 로직이 끝난 다음 Commit을 하기 위함 
	    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
	    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	    // 한번 polling할 때 최대로 가져올 수 있는 record수를 제한 
	    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
	    KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(props);
	    kafkaConsumer.subscribe(Collections.singleton(topicName));
	    System.out.println("Consumer Create Success! = " + topicName);
		return kafkaConsumer;
	}
	
	// create project에서 생성된 schema파일을 읽어들임
	private Schema getSchemaFromFile(String fileName) {
		Schema avroSchema = null;
		try {
			avroSchema = new Schema.Parser().parse(new File("../../create/resources/schema/" + fileName + ".avsc"));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return avroSchema;
	}
	
	public List<GenericRecord> consume(){
		GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
	    BinaryDecoder decoder = null;
	    ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(1000));
	    List<GenericRecord> genericRecords = new ArrayList<GenericRecord>();
	    for (ConsumerRecord<String, byte[]> record : records) {

            decoder = DecoderFactory.get().binaryDecoder(record.value(), decoder);
            GenericRecord genericRecord;
			try {
				genericRecord = reader.read(null, decoder);
				genericRecords.add(genericRecord);
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
	    System.out.println(genericRecords.size() + " record consuming!");
	    return genericRecords;
	}
	
	public void commit() {
		consumer.commitSync();
		System.out.println("Offset Commit!");
	}
	
	public void closeConsumer() {
		consumer.close();
	}
}
