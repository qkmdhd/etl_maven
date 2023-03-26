package com.dhpark.create.creator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

import com.fasterxml.jackson.databind.JsonNode;

public class TableCreator implements Runnable {
	private JsonNode schemaObj;
	private CountDownLatch datasetCount;

	public TableCreator(JsonNode schemaObj, CountDownLatch datasetCount) {
		this.schemaObj = schemaObj;
		this.datasetCount = datasetCount;
	}

	// 타입에 맞게 변경시켜주는 메서드 
	public String createColStr(String name, String type) {
		String typeStr = "";
		switch (type) {
		case "string": {
			typeStr = "VARCHAR(255)";
			break;
		}
		case "long": {
			typeStr = "BIGINT";
			break;
		}
		case "double": {
			typeStr = "DOUBLE";
			break;
		}
		case "integer": {
			typeStr = "INT";
			break;
		}
		case "float": {
			typeStr = "FLOAT";
			break;
		}
		case "bytes": {
			typeStr = "BINARY(8)";
			break;
		}
		case "boolean": {
			typeStr = "BOOLEAN";
			break;
		}
		default: {
			typeStr = "VARCHAR(255)";
			break;
		}
		}
		return name + " " + typeStr + ", ";
	}

	@Override
	public void run() {
		Connection conn = null;
		Statement stmt = null;
		String tableName = schemaObj.get("name").asText();
		JsonNode fieldObj = schemaObj.get("fields");
		Iterator<String> keys = fieldObj.fieldNames();
		// CREATE문 생성
		String qryStr = "CREATE TABLE " + tableName + "(";
		String keyCol = "";
		int index = 0;
		while (keys.hasNext()) {
			String key = keys.next();
			// 0번쨰와 1번쨰 컬럼은 key, timestamp컬럼이라고 가정 
			if (index == 0) {
				keyCol = key;
				qryStr += createColStr(key, fieldObj.get(key).asText());
			} else if (index == 1) {
				qryStr += (key + " TIMESTAMP, ");
			} else {
				qryStr += createColStr(key, fieldObj.get(key).asText());
			}
			index++;
		}
		qryStr += ("PRIMARY KEY (" + keyCol + ")");
		qryStr += ")";

		// Mysql 내에 Table생성
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://192.168.0.165:3306/PDH", "root", "pdhpdh");
			stmt = conn.createStatement();
			stmt.executeUpdate(qryStr);
			System.out.println("create Table success! : " + tableName);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					conn.close();
			} catch (SQLException se) {
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
		datasetCount.countDown();
	}
}
