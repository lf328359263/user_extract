package com.tuyoo.action;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class RedisUser {
	
	private Map<Integer, Pipeline> ps;
	private Map<Integer, Jedis> js;
	
	private void getJedis(){
		this.ps = new HashMap<Integer, Pipeline>();
		this.js = new HashMap<Integer, Jedis>();
		for(int i = 0; i < 8; i++){
			Jedis j = new Jedis("10.3.0.55", 6380+i);
			this.js.put(i, j);
			this.ps.put(i, j.pipelined()) ;
		}
	}
	
	public Map<Integer, String> getUserName(Set<Integer> users){
		Map<Integer, Response<String>> us = new HashMap<Integer, Response<String>>();
		for (Integer u : users) {
			us.put(u, ps.get(u%8).hget("user:"+u, "name"));
		}
		for(Entry<Integer, Pipeline> e : ps.entrySet()){
			e.getValue().sync();
		}
		Map<Integer, String> usernames = new HashMap<Integer, String>();
		for (Entry<Integer, Response<String>> e : us.entrySet()) {
			usernames.put(e.getKey(), e.getValue().get());
		}
		return usernames;
	}
	
	public String getUserName(Integer userid){
		return js.get(userid%8).hget("user:"+userid, "name");
	}
	
	public RedisUser(){
		super();
		this.getJedis();
	}
	
	public void close(){
		for (Entry<Integer, Jedis> e : this.js.entrySet()) {
			e.getValue().close();
		}
	}
}
