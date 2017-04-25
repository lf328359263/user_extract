package com.tuyou.util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

public class Util {
	private static Logger logger = Logger.getLogger(Util.class);

	public static Connection getMysqlConn() throws ClassNotFoundException, SQLException {
		 Class.forName("com.mysql.jdbc.Driver");
		 return DriverManager.getConnection("jdbc:mysql://10.3.0.50:3307/result_test?useUnicode=true&characterEncoding=UTF-8", "tuyoo", "tuyougame");
	}

	public static Connection getHiveConn() throws ClassNotFoundException, SQLException {
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		return DriverManager.getConnection("jdbc:hive2://10.3.0.51:21050/;auth=noSasl");
	}
	
	public static String getIpAddress(long ipaddress) {  
        StringBuffer sb = new StringBuffer("");  
        sb.append(String.valueOf((ipaddress >>> 24)));  
        sb.append(".");  
        sb.append(String.valueOf((ipaddress & 0x00FFFFFF) >>> 16));  
        sb.append(".");  
        sb.append(String.valueOf((ipaddress & 0x0000FFFF) >>> 8));  
        sb.append(".");  
        sb.append(String.valueOf((ipaddress & 0x000000FF)));  
        return sb.toString();  
    }
	
	public static String getLineByHttp(){
		String url = "http://10.3.12.16:8012/configuration/entry/6152/export";
		HttpGet request = new HttpGet(url);
		HttpResponse response;
		String line = "";
		try {
			response = HttpClients.createDefault().execute(request);
			if(response.getStatusLine().getStatusCode()==200){
				HttpEntity entity = response.getEntity();
				line = EntityUtils.toString(entity);
			} 
		} catch (IOException e) {
			e.printStackTrace();
		}
		return line;
	}
	
	public static long ip2int(String ip){
		String[] items=ip.split("\\.");
		return Long.valueOf(items[0])<<24|Long.valueOf(items[1])<<16|Long.valueOf(items[2])<<8|Long.valueOf(items[3]);
	} 
	
	public static String getAllLongIp(){
		String line = getLineByHttp();
		StringBuffer sb = new StringBuffer("'',");
		String[] split = line.split("\"|,");
		for (String ip : split) {
			if(ip.contains(".")){
				sb.append("'").append(ip2int(ip)).append("',");
			}
		}
		return sb.substring(0, sb.length()-1);
	}
	
	public static String getIpAddr(long ipaddress) {  
        StringBuffer sb = new StringBuffer();  
        sb.append(String.valueOf((ipaddress >>> 24)));  
        sb.append(".");  
        sb.append(String.valueOf((ipaddress & 0x00FFFFFF) >>> 16));   
        return sb.toString();  
    }
	
	public static void refreshMata() {
		Connection hive = null;
		try {
			hive = Util.getHiveConn();
			hive.createStatement().execute("invalidate metadata");
			logger.info("刷新元数据成功");
		} catch (ClassNotFoundException | SQLException e1) {
			logger.info("刷新元数据成功失败");
			e1.printStackTrace();
		} finally {
			connClose(hive);
		}
	}

	public static void connClose(Connection conn) {
		try {
			if (null != conn)
				conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
