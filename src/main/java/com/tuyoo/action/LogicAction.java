package com.tuyoo.action;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.tuyou.util.Util;

public class LogicAction {
	
	private Integer id;
	private Integer clientid;
	private String day;
	private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private RedisUser ru ;
	private static Logger LOG = Logger.getLogger(LogicAction.class);
	private Connection conn;
	private Integer count = 0;
//	Map<String, Integer> IN = new HashMap<String, Integer>();
	Map<String, Integer> I = new HashMap<String, Integer>();
//	Map<String, Integer> N = new HashMap<String, Integer>();
	private Integer taskId;
	
	public LogicAction(){
		super();
		ru = new RedisUser();
		try {
			conn = Util.getMysqlConn();
			LOG.info("初始化mysql连接。。。");
		} catch (ClassNotFoundException | SQLException e) {
			LOG.error(e.getMessage());
		}
	}
	
	public LogicAction(Integer id, Integer clientid, String day, Integer taskId){
		this();
		this.id = id;
		this.clientid = clientid;
		this.day = day;
		this.taskId = taskId;
	}
	
	/**
	 * 执行程序
	 */
	public void exe(){
//		1、读出执行sql；
		String hql = this.getSql();
//		2、执行sql；
		ResultSet exeImpala = this.exeImpala(hql);
//		3、日志结果写入库；
		this.insert(exeImpala);
//		4、分析结果；
		this.analyse();
		String sql = "select user_id, create_date, create_time, create_clientid, ip_address, ip_addr, user_name from user_tmp_log";
		ResultSet result = this.exeMysql(sql);
		reduce(result);
		this.close();
	}
	
	/**
	 * 读取mysql中的sql语句， 并赋值
	 * @return sql
	 */
	public String getSql(){
		String sql = "";
		String query = "select exe_sql from user_sql_hive where id = " + id;
		ResultSet executeQuery = this.exeMysql(query);
		try {
			while(executeQuery.next()){
				sql = executeQuery.getString(1);
			}
		} catch (SQLException e) {
			LOG.error(e.getMessage());
		}
		String ips = Util.getAllLongIp();
		sql = sql.replaceAll("\\$client_id\\$", String.valueOf(this.clientid)).replaceAll("\\$day\\$", this.day).replaceAll("\\$ips\\$", ips);
		LOG.info("执行sql语句为：\t"+sql);
		return sql;
	}
	
	/**
	 * impala 执行 sql
	 * @param sql
	 * @return resultSet
	 */
	private ResultSet exeImpala(String sql){
		Statement statement;
		ResultSet resultSet = null;
		try {
			LOG.info("impala 执行sql。。。" + sql);
			statement = Util.getHiveConn().createStatement();
			resultSet = statement.executeQuery(sql);
			statement.close();
		} catch (ClassNotFoundException | SQLException e) {
			LOG.error(e.getMessage());
		}
		LOG.info("impala执行完毕。。。");
		return resultSet;
	}
	
	/**
	 * 在mysql中，执行sql
	 * @param sql
	 * @return
	 */
	private ResultSet exeMysql(String sql){
		Statement statement;
		ResultSet resultSet = null;
		try {
			LOG.info("准备执行sql。。。" + sql);
			statement = conn.createStatement();
			resultSet = statement.executeQuery(sql);
			statement.close();
		} catch (SQLException e) {
			LOG.error(e.getMessage());
		}
		LOG.info("sql执行完毕。。。" + sql);
		return resultSet;
	}
	
	/**
	 * log写入数据库
	 * @param resultSet
	 */
	private void insert(ResultSet resultSet){
		String sql = "insert into user_tmp_log (user_id, create_date, create_time, create_clientid, ip_address, ip_addr, user_name, type) values (?,?,?,?,?,?,?,?)";
		try {
			LOG.info("临时数据写入mysql。。。");
			conn.setAutoCommit(false);
			PreparedStatement statement = conn.prepareStatement(sql);
			while(resultSet.next()){
				int userid = resultSet.getInt("user_id");
				long eventtime = resultSet.getLong("event_time");
				eventtime*=1000;
				long ip = resultSet.getLong("ip_addr");
				statement.setInt(1, userid);
				statement.setString(2, sdf.format(new Date(eventtime)));
				statement.setTimestamp(3, new Timestamp(eventtime));
				statement.setInt(4, resultSet.getInt("client_id"));
				statement.setString(5, Util.getIpAddress(ip));
				statement.setString(6, Util.getIpAddr(ip));
				statement.setString(7, ru.getUserName(userid));
				statement.setInt(8, id);
				statement.addBatch();
				count++;
			}
			statement.executeBatch();
			statement.clearBatch();
			conn.commit();
		} catch (SQLException e) {
			LOG.error(e.getMessage());
		}
		LOG.info("写入数据："+ this.count);
		LOG.info("临时数据写入mysql完毕。。。");
	}
	
	/**
	 * 分析结果写入数据库
	 * @param resultSet
	 */
	private void reduce(ResultSet resultSet){
		String sql = "insert into illegal_users (user_id, create_date, create_time, create_clientid, ip_address, ip_addr, user_name, ip_name_num, ip_name_percent, ip_num, ip_percent, name_num, name_percent, task_id) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		try {
			LOG.info("准备写入mysql。。。");
			conn.setAutoCommit(false);
			PreparedStatement statement = conn.prepareStatement(sql);
			while(resultSet.next()){
				String ip = resultSet.getString("ip_addr");
//				String name = resultSet.getString("user_name");
				statement.setInt(1, resultSet.getInt("user_id"));
				statement.setString(2, resultSet.getString("create_date"));
				statement.setTimestamp(3, resultSet.getTimestamp("create_time"));
				statement.setString(4, resultSet.getString("create_clientid"));
				statement.setString(5, resultSet.getString("ip_address"));
				statement.setString(6, resultSet.getString("ip_addr"));
				statement.setString(7, resultSet.getString("user_name"));
//				statement.setInt(10, I.get(ip));
//				statement.setFloat(11, ((float)I.get(ip))/count);
//				if(IN.containsKey(ip+"."+name)){
//					statement.setInt(8, IN.get(ip+"."+name));
//					statement.setFloat(9, ((float)IN.get(ip+"."+name))/count);
//				} else {
//					System.out.println(ip+"."+name);
					statement.setInt(8, 0);
					statement.setFloat(9, ((float)0)/count);
//				}
				if(I.containsKey(ip)){
					statement.setInt(10, I.get(ip));
					statement.setFloat(11, ((float)I.get(ip))/count);
				} else {
					System.out.println(ip);
					statement.setInt(10, 0);
					statement.setFloat(11, ((float)0)/count);
				}
//				if(N.containsKey(name)){
//					statement.setInt(12, N.get(name));
//					statement.setFloat(13, ((float)N.get(name))/count);
//				} else {
//					System.out.println(name);
					statement.setInt(12, 0);
					statement.setFloat(13, ((float)0)/count);
//				}
				statement.setInt(14, this.taskId);
				statement.addBatch();
			}
			statement.executeBatch();
			statement.clearBatch();
			conn.commit();
		} catch (SQLException e) {
			LOG.error(e.getMessage());
		}
		LOG.info("mysql log 写入完毕。。。");
	}
	
	private void analyse(){
//		String ipAndName = "select ip_addr, user_name, count(user_id) value from user_tmp_log group by ip_addr, user_name";
		String ip = "select ip_addr, count(user_id) value from user_tmp_log group by ip_addr";
//		String name = "select user_name, count(user_id) value from user_tmp_log group by user_name";
//		ResultSet in = this.exeMysql(ipAndName);
		ResultSet i = this.exeMysql(ip);
//		ResultSet n = this.exeMysql(name);
		try {
//			while (in.next()) {
//				IN.put(in.getString(1)+"."+in.getString(2), in.getInt(3));
//			}
			while (i.next()) {
				I.put(i.getString(1), i.getInt(2));
			}
//			while (n.next()) {
//				N.put(n.getString(1), n.getInt(2));
//			}
		} catch (SQLException e) {
			LOG.error(e.getMessage());
		}
		
	}
	
	private void close(){
		String sql = "truncate table user_tmp_log";
		try {
			conn.createStatement().execute(sql);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
//		this.exeMysql(sql);
		LOG.info("清空临时表。。。");
		this.ru.close();
		LOG.info("关闭Redis连接。。。");
		try {
			this.conn.close();
			LOG.info("关闭mysql连接。。。");
		} catch (SQLException e) {
			LOG.error(e.getMessage());
		}

	}
	
	public void tmp(){
		this.analyse();
		this.taskId= 76;
		String sql = "select user_id, create_date, create_time, create_clientid, ip_address, ip_addr, user_name from user_tmp_log";
		
		ResultSet result = this.exeMysql(sql);
		this.reduce(result);
		
		this.close();
	}
	
}
