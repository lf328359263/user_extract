package com.tuyoo.httptest;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;


public class HTTPGet {
	
	public static String getLineByHttp(String url){
//		String url = "http://10.3.12.16:8012/configuration/entry/6152/export";
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
	
	public static String getAllLongIp(String line){
		StringBuffer sb = new StringBuffer("'',");
		String[] split = line.split("\"|,");
		for (String ip : split) {
			if(ip.contains(".")){
				sb.append("'").append(ip2int(ip)).append("',");
			}
		}
		return sb.substring(0, sb.length()-1);
	}
	
	public static void main(String[] args) {
		String url = "http://localhost:9080/datacenter/test";
		System.out.println(getLineByHttp(url));
	}
}
