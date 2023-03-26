package com.dhpark.create.creator;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.NewTopic;

import com.fasterxml.jackson.databind.JsonNode;

public class TopicCreator implements Runnable {
	private JsonNode schemaObj;
	private CountDownLatch datasetCount;

	public TopicCreator(JsonNode schemaObj, CountDownLatch datasetCount) {
		this.schemaObj = schemaObj;
		this.datasetCount = datasetCount;
	}

	@Override
	public void run() {

		Properties props = new Properties();
		props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
				"192.168.0.165:9092,192.168.0.165:9093,192.168.0.165:9094");

		try (AdminClient adminClient = AdminClient.create(props)) {

			//node의 수 만큼 partition 생성을 위해 
			DescribeClusterResult describeClusterResult = adminClient.describeCluster();
			int nodesCount = 0;
			nodesCount = describeClusterResult.nodes().get().size();

			String topicName = schemaObj.get("name").asText();
			// 생성할 토픽 정보 설정 replicationFactor -> 노드의 수만큼 설정하여 안정을 높임 
			NewTopic newTopic = new NewTopic(topicName, nodesCount, (short) nodesCount);

			// 토픽 생성 요청
			adminClient.createTopics(Collections.singleton(newTopic));

			System.out.println("create Topic success! : " + topicName);

		} catch (Exception e) {
			e.printStackTrace();
		}
		datasetCount.countDown();
	}
}
