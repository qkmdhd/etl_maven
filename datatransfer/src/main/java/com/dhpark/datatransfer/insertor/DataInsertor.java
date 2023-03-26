package com.dhpark.datatransfer.insertor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;

public class DataInsertor {
	private Connection conn;
	private String tableName;

	public DataInsertor(String tableName) {
		this.tableName = tableName;
		this.conn = createConnection();
	}

	private Connection createConnection() {
		Connection connection = null;
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			connection = DriverManager.getConnection("jdbc:mysql://192.168.0.165:3306/PDH", "root", "pdhpdh");
			connection.setAutoCommit(false);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return connection;
	}

	public void insert(List<GenericRecord> records) throws Exception {
		if (records.size() == 0) {
			return;
		}
		PreparedStatement pstmt = null;
		try {
			StringBuilder queryBuilder = new StringBuilder("INSERT INTO " + tableName + " (");
			StringBuilder valueBuilder = new StringBuilder(" VALUES (");
			for (Field field : records.get(0).getSchema().getFields()) {
				queryBuilder.append(field.name()).append(", ");
				valueBuilder.append("?, ");
			}
			queryBuilder.delete(queryBuilder.length() - 2, queryBuilder.length()).append(")");
			valueBuilder.delete(valueBuilder.length() - 2, valueBuilder.length()).append(")");
			pstmt = conn.prepareStatement(queryBuilder.toString() + valueBuilder.toString());
			for (GenericRecord record : records) {
				int index = 1;
				for (Field field : record.getSchema().getFields()) {
					Object value = record.get(field.name());
					// 0번쨰와 1번쨰 컬럼은 key, timestamp컬럼이라고 가정
					if (field.pos() == 1) {
						pstmt.setTimestamp(index++, new Timestamp((long) value));
					} else {
						switch (field.schema().getType().toString()) {

						case "STRING": {
							pstmt.setString(index++, value.toString());
							break;
						}
						case "INT": {
							pstmt.setInt(index++, (int) value);
							break;
						}
						case "DOUBLE": {
							pstmt.setDouble(index++, (double) value);
							break;
						}
						case "LONG": {
							pstmt.setLong(index++, (long) value);
							break;
						}
						case "FLOAT": {
							pstmt.setFloat(index++, (float) value);
							break;
						}
						case "BYTES": {
							pstmt.setBytes(index, (byte[]) value);
							break;
						}
						case "BOOLEAN": {
							pstmt.setBoolean(index, (boolean) value);
							break;
						}
						default: {
							pstmt.setObject(index++, value);
						}
						}
					}

				}
				pstmt.addBatch();
			}
			pstmt.executeBatch();
			conn.commit();
		} catch (Exception e) {
			System.out.println("Insert Failed!");
			try {
				if (conn != null) {
					conn.rollback();
				}
			} catch (SQLException e1) {
				e1.printStackTrace();
				throw new Exception(e);
			}
			e.printStackTrace();
			throw new Exception(e);
		} finally {
			try {
				if (pstmt != null) {
					pstmt.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public void close() {

		try {
			if (conn != null) {
				conn.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
}
