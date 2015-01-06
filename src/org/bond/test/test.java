package org.bond.test;

import org.bond.util.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class test {

	public static void main(String[] args) {
		try {

			int type = 0;
			switch (type) {
			case 0:
				test0();
				break;
			case 1:
				test1();
				break;
			case 2:
				test2();
				break;
			case 3:
				test3();
				break;
			case 4:
				test4();
				break;
			default:
				System.out.print("----------默认---------");
				break;

			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	private static void test0() {
		for (int i = 0; i < 80; i++) {

			short a = 10;

			PerformanceTest p = new PerformanceTest();
			Thread t = new Thread(p);
			t.start();
		}
	}

	private static void test1() throws SQLException {
		// 手动连接
		System.out.println("-------- Oracle JDBC Connection Testing ------");
		long start = System.currentTimeMillis();
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			System.out.println("Where is your Oracle JDBC Driver?");
			e.printStackTrace();
			return;
		}

		System.out.println("Oracle JDBC Driver Registered!");
		Connection connection = null;

		try {

			connection = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:orcl", "scott", "dreamsoft");

			String sql = "select dname from dept";
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				System.out.println(rs.getObject(1).toString());
			}
			rs.close();

			long end = System.currentTimeMillis();
			System.out.println("------------耗时:" + (end - start) + "毫秒-------------");

		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
		} finally {
			if (connection != null) {
				connection.close();
			}
		}
	}

	private static void test2() throws Exception {
		String sql = "select * from dept";
		// 测试1
		long start = System.currentTimeMillis();
		System.out.println("------------oracle数据库-------------");
		System.out.println("------------方式1-------------");
		Connection conn = ConnectionPool.getConnection("oracle");
		// execute business operations
		Statement stmt = null;
		ResultSet rset = null;
		stmt = conn.createStatement();
		rset = stmt.executeQuery(sql);
		int cols = rset.getMetaData().getColumnCount();
		while (rset.next()) {
			for (int i = 1; i <= cols; i++) {
				System.out.print("\t" + rset.getObject(i));
			}
			System.out.println("");
		}
		ConnectionPool.close(rset, stmt, conn);
		long end = System.currentTimeMillis();
		System.out.println("------------耗时:" + (end - start) + "毫秒-------------");

		// 测试2
		System.out.println("------------方式2-------------");
		start = System.currentTimeMillis();
		List<Map<String, Object>> list = DBUtil.query("oracle", sql);
		for (Map<String, Object> item : list) {
			Iterator<Object> it = item.values().iterator();
			while (it.hasNext()) {
				System.out.print(it.next());
				System.out.print("\t");
			}

			System.out.println();
		}
		end = System.currentTimeMillis();
		System.out.println("------------耗时:" + (end - start) + "毫秒-------------");

		// 测试3
		System.out.println("------------方式3-------------");
		start = System.currentTimeMillis();
		sql = "select * from dept where deptno=?";
		list = DBUtil.query("oracle", sql, 10);
		for (Map<String, Object> item : list) {
			Iterator<Object> it = item.values().iterator();
			while (it.hasNext()) {
				System.out.print(it.next());
				System.out.print("\t");
			}

			System.out.println();
		}
		end = System.currentTimeMillis();
		System.out.println("------------耗时:" + (end - start) + "毫秒-------------");

		// 测试4
		System.out.println("------------方式4-------------");
		start = System.currentTimeMillis();
		sql = "select * from dept where deptno=? and dname=?";
		list = DBUtil.queryParam("oracle", sql, 10, "ACCOUNTING");
		for (Map<String, Object> item : list) {
			Iterator<Object> it = item.values().iterator();
			while (it.hasNext()) {
				System.out.print(it.next());
				System.out.print("\t");
			}

			System.out.println();
		}
		end = System.currentTimeMillis();
		System.out.println("------------耗时:" + (end - start) + "毫秒-------------");

		// 测试5
		System.out.println("------------方式5-------------");
		start = System.currentTimeMillis();
		sql = "update dept set dname=? where deptno=?";
		DBUtil.executeParam("oracle", sql, "测试部门", 20);
		sql = "select dname from dept where deptno=?";
		Object val = DBUtil.queryScalar("oracle", sql, 20);

		System.out.println("修改后的结果：" + val);
		end = System.currentTimeMillis();
		System.out.println("------------耗时:" + (end - start) + "毫秒-------------");
	}

	private static void test3() throws Exception {
		// if exists(select 1 from sys.tables where name='dept' and
		// schema_id=(select schema_id from sys.schemas where name='dbo'))
		// begin
		// drop table dept
		// end
		// go
		// create table dept
		// (
		// deptno int not null,
		// dname varchar(512),
		// loc varchar(512)
		// )
		// go
		// alter table dept add constraint pk_dept primary key(deptno)
		// go
		// insert into dept(deptno,dname,loc) values(10,'ACCOUNTING','洛杉矶')
		// go
		// insert into dept(deptno,dname,loc) values(20,'测试部','纽约')
		// go
		// insert into dept(deptno,dname,loc) values(30,'开发部','新德里')
		// go
		// insert into dept(deptno,dname,loc) values(40,'财务部','北京')
		// go

		String sql = "select * from dept";
		// 测试1
		System.out.println("------------sqlserver数据库-------------");
		System.out.println("------------方式1-------------");
		long start = System.currentTimeMillis();
		Connection conn = ConnectionPool.getConnection("sqlserver");
		// execute business operations
		Statement stmt = null;
		ResultSet rset = null;
		stmt = conn.createStatement();
		rset = stmt.executeQuery(sql);
		int cols = rset.getMetaData().getColumnCount();
		while (rset.next()) {
			for (int i = 1; i <= cols; i++) {
				System.out.print("\t" + rset.getObject(i));
			}
			System.out.println("");
		}
		ConnectionPool.close(rset, stmt, conn);
		long end = System.currentTimeMillis();
		System.out.println("------------耗时:" + (end - start) + "毫秒-------------");

		// 测试2
		System.out.println("------------方式2-------------");
		start = System.currentTimeMillis();
		List<Map<String, Object>> list = DBUtil.query("sqlserver", sql);
		for (Map<String, Object> item : list) {
			Iterator<Object> it = item.values().iterator();
			while (it.hasNext()) {
				System.out.print(it.next());
				System.out.print("\t");
			}

			System.out.println();
		}
		end = System.currentTimeMillis();
		System.out.println("------------耗时:" + (end - start) + "毫秒-------------");

		// 测试3
		System.out.println("------------方式3-------------");
		start = System.currentTimeMillis();
		sql = "select * from dept where deptno=?";
		list = DBUtil.query("sqlserver", sql, 10);
		for (Map<String, Object> item : list) {
			Iterator<Object> it = item.values().iterator();
			while (it.hasNext()) {
				System.out.print(it.next());
				System.out.print("\t");
			}

			System.out.println();
		}
		end = System.currentTimeMillis();
		System.out.println("------------耗时:" + (end - start) + "毫秒-------------");

		// 测试4
		System.out.println("------------方式4-------------");
		start = System.currentTimeMillis();
		sql = "select * from dept where deptno=? and dname=?";
		list = DBUtil.queryParam("sqlserver", sql, 10, "ACCOUNTING");
		for (Map<String, Object> item : list) {
			Iterator<Object> it = item.values().iterator();
			while (it.hasNext()) {
				System.out.print(it.next());
				System.out.print("\t");
			}

			System.out.println();
		}
		end = System.currentTimeMillis();
		System.out.println("------------耗时:" + (end - start) + "毫秒-------------");

		// 测试5
		System.out.println("------------方式5-------------");
		start = System.currentTimeMillis();
		sql = "update dept set dname=? where deptno=?";
		DBUtil.executeParam("sqlserver", sql, "测试部门", 20);
		sql = "select dname from dept where deptno=?";
		Object val = DBUtil.queryScalar("sqlserver", sql, 20);

		System.out.println("修改后的结果：" + val);
		end = System.currentTimeMillis();
		System.out.println("------------耗时:" + (end - start) + "毫秒-------------");
	}

	private static void test4() throws Exception {
		String sql = "select * from dept";
		// 测试1
		System.out.println("------------mysql数据库-------------");
		System.out.println("------------方式1-------------");
		long start = System.currentTimeMillis();
		Connection conn = ConnectionPool.getConnection("mysql");
		// execute business operations
		Statement stmt = null;
		ResultSet rset = null;
		stmt = conn.createStatement();
		rset = stmt.executeQuery(sql);
		int cols = rset.getMetaData().getColumnCount();
		while (rset.next()) {
			for (int i = 1; i <= cols; i++) {
				System.out.print("\t" + rset.getObject(i));
			}
			System.out.println("");
		}
		ConnectionPool.close(rset, stmt, conn);
		long end = System.currentTimeMillis();
		System.out.println("------------耗时:" + (end - start) + "毫秒-------------");

		// 测试2
		System.out.println("------------方式2-------------");
		start = System.currentTimeMillis();
		List<Map<String, Object>> list = DBUtil.query("mysql", sql);
		for (Map<String, Object> item : list) {
			Iterator<Object> it = item.values().iterator();
			while (it.hasNext()) {
				System.out.print(it.next());
				System.out.print("\t");
			}

			System.out.println();
		}
		end = System.currentTimeMillis();
		System.out.println("------------耗时:" + (end - start) + "毫秒-------------");

		// 测试3
		System.out.println("------------方式3-------------");
		start = System.currentTimeMillis();
		sql = "select * from dept where deptno=?";
		list = DBUtil.query("mysql", sql, 10);
		for (Map<String, Object> item : list) {
			Iterator<Object> it = item.values().iterator();
			while (it.hasNext()) {
				System.out.print(it.next());
				System.out.print("\t");
			}

			System.out.println();
		}
		end = System.currentTimeMillis();
		System.out.println("------------耗时:" + (end - start) + "毫秒-------------");

		// 测试4
		System.out.println("------------方式4-------------");
		start = System.currentTimeMillis();
		sql = "select * from dept where deptno=? and dname=?";
		list = DBUtil.queryParam("mysql", sql, 10, "ACCOUNTING");
		for (Map<String, Object> item : list) {
			Iterator<Object> it = item.values().iterator();
			while (it.hasNext()) {
				System.out.print(it.next());
				System.out.print("\t");
			}

			System.out.println();
		}
		end = System.currentTimeMillis();
		System.out.println("------------耗时:" + (end - start) + "毫秒-------------");

		// 测试5
		System.out.println("------------方式5-------------");
		start = System.currentTimeMillis();
		sql = "update dept set dname=? where deptno=?";
		DBUtil.executeParam("mysql", sql, "测试部门", 20);
		sql = "select dname from dept where deptno=?";
		Object val = DBUtil.queryScalar("mysql", sql, 20);

		System.out.println("修改后的结果：" + val);
		end = System.currentTimeMillis();
		System.out.println("------------耗时:" + (end - start) + "毫秒-------------");
	}

}
