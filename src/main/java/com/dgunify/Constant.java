package com.dgunify;

import java.util.Properties;

public class Constant {
	public static Properties connectionProperties;
	public static String url = "jdbc:mysql://127.0.0.1:3306/users";
	static {
		connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password","root");
        connectionProperties.put("driver", "com.mysql.jdbc.Driver");
	}
}
