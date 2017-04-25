package com.tuyoo.action;

import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;
import javax.xml.ws.Endpoint;

@WebService
public class IllegalUsers {
	
	@WebMethod
	public Integer loadUsers(@WebParam(name="id")Integer id, @WebParam(name="client_id")Integer client_id, @WebParam(name="day")String day, @WebParam(name="taskId")Integer taskId){
		LogicAction la = new LogicAction(id, client_id, day, taskId);
		la.exe();
		return 1;
	}
	
	public static void main(String[] args) {
		String url = "http://10.3.0.50:9009/loadUser";
		Endpoint.publish(url, new IllegalUsers());
		System.out.println("发布地址：\t"+url);
	}
}
