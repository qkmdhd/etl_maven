package com.dhpark.create;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.dhpark.create.service.CreateService;

public class CreateMain {

	private static String jsonStr;
	
	public static void main(String[] args) {
		
		preProcess();

		CreateService handler = new CreateService();
		// AvroSchema 생성 
		handler.create(jsonStr, "schema");
		System.out.println("Create Avro Schema is Success!");
		// Kafka Topic 생성 
		handler.create(jsonStr, "topic");
		System.out.println("Create Kafka Topic is Success!");
		// Mysql Table 생
		handler.create(jsonStr, "table");
		System.out.println("Create Mysql Table is Success!");
		
	}
	
	
	// schema.json파일을 읽어 main에서 handler에 넘기기위해 전처리하는 메서드
	public static void preProcess() {
		String fileName = "schema.json";
		
		// 파일 경로를 Classpath로부터 얻어옴 
		String filePath = CreateMain.class.getClassLoader().getResource(fileName).getPath();
        
        // 파일 내용을 문자열에 할당 
        try {
			jsonStr = new String(Files.readAllBytes(Paths.get(filePath)), StandardCharsets.UTF_8);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
