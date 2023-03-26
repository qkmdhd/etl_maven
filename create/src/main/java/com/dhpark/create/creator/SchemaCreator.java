package com.dhpark.create.creator;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class SchemaCreator implements Runnable {
	private JsonNode schemaObj;
    private CountDownLatch datasetCount;

    public SchemaCreator(JsonNode schemaObj, CountDownLatch datasetCount) {
        this.schemaObj = schemaObj;
        this.datasetCount = datasetCount;
    }

    public ObjectNode createSchemaJson() {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode schemaJson = mapper.createObjectNode();
        schemaJson.put("type", "record");
        schemaJson.put("name", schemaObj.get("name").asText());
        ArrayNode fieldArr = mapper.createArrayNode();
        JsonNode fieldObj = schemaObj.get("fields");
        Iterator<String> keys = fieldObj.fieldNames();
        while (keys.hasNext()) {
            String key = keys.next();
            ObjectNode schemaField = mapper.createObjectNode();
            schemaField.put("name", key);
            schemaField.put("type", fieldObj.get(key).asText().equals("integer")?"int":fieldObj.get(key).asText());
            fieldArr.add(schemaField);
        }
        schemaJson.set("fields", fieldArr);
        return schemaJson;
    }

	public void run() {
		
		ObjectNode schemaJson = createSchemaJson();
		
        Path resourcesDirectory = Paths.get("../resources");
        
        // Schema 디렉토리 경로 생성
        Path schemaDirectory = Paths.get(resourcesDirectory.toString(), "schema");
        
        // Schema 디렉토리가 없으면 생성
        if (!Files.exists(schemaDirectory)) {
            try {
                Files.createDirectories(schemaDirectory);
            } catch (IOException e) {
                System.out.println("Failed to create directory: " + schemaDirectory);
                return;
            }
        }
        
        // avsc파일 생성 
        Path filePath = Paths.get(schemaDirectory.toString(), schemaJson.get("name").asText() + ".avsc");
        try {
            FileWriter fileWriter = new FileWriter(filePath.toFile());
            fileWriter.write(schemaJson.toString());
            fileWriter.close();
        } catch (IOException e) {
            System.out.println("Failed to create file: " + filePath);
            return;
        }
        System.out.println("create file success! : " + filePath);
		// 비즈니스로직이 완료 된 뒤 CountDown 
		datasetCount.countDown();
	}
}
