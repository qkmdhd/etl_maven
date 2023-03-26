package com.dhpark.datatransfer.service;

import com.dhpark.datatransfer.consumer.DataConsumer;
import com.dhpark.datatransfer.insertor.DataInsertor;

public class TransferService implements Runnable {
	private String topicName;
	
	public TransferService(String topicName) {
		this.topicName = topicName;
	}
	
	public void run() {
		DataConsumer consumer = new DataConsumer(topicName);
		DataInsertor insertor = new DataInsertor(topicName);
		// Data를 Consume해서 Table에 Insert
		try {
			while(true) {
				insertor.insert(consumer.consume());
				consumer.commit();
			}
		} catch(Exception e) {
			System.out.println("Insert Error! So Offset is not Commit");
			insertor.close();
			e.printStackTrace();
			System.exit(0);
		}
	}
}
