package com.dhpark.datagen.generator;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.security.SecureRandom;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

public class DataGenerator {

	private Schema schema;

	public DataGenerator(String fileName) {
		getSchemaFromFile(fileName);
	}
	
	// create project에서 생성된 schema파일을 읽어들임 
	private void getSchemaFromFile(String fileName) {
		try {
			this.schema = new Schema.Parser().parse(new File("../../create/resources/schema/" + fileName + ".avsc"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public ByteArrayOutputStream generateData() {
		GenericRecord avroRecord = new GenericData.Record(schema);
		schema.getFields().forEach(field -> {
			Object value = null;
			Random random = new Random();
			// 0번쨰와 1번쨰 컬럼은 key, timestamp컬럼이라고 가정
			if (field.pos() == 0) {
				value = "Key_" + generateKey(15);
			} else if (field.pos() == 1) {
				value = System.currentTimeMillis();
			} else {
				switch (field.schema().getType().toString()) {

				case "STRING": {
					value = generateKey(10);
					break;
				}
				case "INT": {
					value = random.nextInt();
					break;
				}
				case "DOUBLE": {
					value = random.nextDouble();
					break;
				}
				case "LONG": {
					value = random.nextLong();
					break;
				}
				case "FLOAT": {
					value = random.nextFloat();
					break;
				}
				case "BYTES": {
					byte[] bytes = new byte[10];
					random.nextBytes(bytes);
					value = bytes;
					break;
				}
				case "BOOLEAN": {
					value = random.nextBoolean();
					break;
				}
				default: {
					value = null;
				}

				}
			}
			avroRecord.put(field.name(), value);
			
		});
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
		try {
			writer.write(avroRecord, encoder);
			encoder.flush();
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return out;
	}
	
	// 0번째 컬럼의 Key값, String 타입의 값들을 생성하기 위해 랜덤한 문자열 생성해주는 메서
	public String generateKey(int length) {
        StringBuilder sb = new StringBuilder(length);
        String CHAR_UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        String CHAR_LOWER = "abcdefghijklmnopqrstuvwxyz";
        Random RANDOM = new SecureRandom();
        for (int i = 0; i < length; i++) {
            if (RANDOM.nextBoolean()) {
                sb.append(CHAR_UPPER.charAt(RANDOM.nextInt(CHAR_UPPER.length())));
            } else {
                sb.append(CHAR_LOWER.charAt(RANDOM.nextInt(CHAR_LOWER.length())));
            }
        }
        return sb.toString();
    }
}
