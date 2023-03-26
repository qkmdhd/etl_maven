package com.dhpark.datatransfer.util;

import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;

public class KafkaUtil {
	// Broker수를 구하는 메서드 
	public static int getNodeCount() {
		Properties props = new Properties();
		props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
				"192.168.0.165:9092,192.168.0.165:9093,192.168.0.165:9094");

		int nodesCount = 0;
		try (AdminClient adminClient = AdminClient.create(props)) {

			//node의 수 만큼 partition 생성을 위해 
			DescribeClusterResult describeClusterResult = adminClient.describeCluster();
			nodesCount = describeClusterResult.nodes().get().size();
		}catch(Exception e) {
			e.printStackTrace();
		}
        return nodesCount;
    }
}
