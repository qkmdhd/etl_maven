package com.dhpark.datagen;

import java.io.File;
import java.util.Arrays;

import com.dhpark.datagen.service.GenerateService;

public class GenerateMain {

	private static String fileName;
	
	public static void main(String[] args) {
		preProcess();
		String[] fileNameArr = fileName.split(",");
		// 구분자로 나누어진 파일명들을 나누어 각 스레드 실행 
		for(String fileName : fileNameArr) {
			GenerateService service = new GenerateService(fileName);
			Thread serviceThread = new Thread(service);
			serviceThread.start();
		}
	}
	
	// 파일명, topic명, table명이 같기때문에 파일명을 읽어들여 "," 구분자로 이어준 뒤 추후 분리하여 각 스레드로 전달 
	private static void preProcess() {
		String path = "../../create/resources/schema";
        File directory = new File(path);
        File[] files = directory.listFiles();
        if (files != null) {
            StringBuilder sb = new StringBuilder();
            Arrays.stream(files).forEach(file -> {
                String fileName = file.getName();
                int index = fileName.lastIndexOf('.');
                if (index > 0 && index < fileName.length() - 1) {
                    fileName = fileName.substring(0, index);
                }
                sb.append(fileName).append(",");
            });
            sb.setLength(sb.length() - 1);
            fileName = sb.toString();
        }
        System.out.println("fileName = " + fileName);
	}

}
