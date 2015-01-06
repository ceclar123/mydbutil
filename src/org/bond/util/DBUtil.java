package org.bond.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;

public class DBUtil {

	/**
	 * 执行sql语句
	 * 
	 * @param sql
	 *            被执行的sql语句
	 * @return 受影响的行
	 * @throws Exception
	 */
	public static int execute(String sql) throws Exception {
		return execute(null, sql);
	}

	/**
	 * 执行sql语句
	 * 
	 * @param dbType
	 *            数据库类型oracle,sqlserver,mysql
	 * @param sql
	 *            被执行的sql语句
	 * @return 受影响的行
	 * @throws Exception
	 */
	public static int execute(String dbType, String sql) throws Exception {
		Connection conn = null;
		int rows = 0;
		try {
			if (dbType == null || dbType.length() == 0) {
				conn = ConnectionPool.getConnection();
			} else {
				conn = ConnectionPool.getConnection(dbType);
			}

			QueryRunner qr = new QueryRunner();
			rows = qr.update(conn, sql);
		} finally {
			ConnectionPool.close(conn);
		}
		return rows;
	}

	/**
	 * 执行含参数的sql语句
	 * 
	 * @param sql
	 *            被执行的sql语句
	 * @param params
	 *            参数数组
	 * @return 返回受影响的行
	 * @throws Exception
	 */
	public static int execute(String sql, Object[] params) throws Exception {
		return execute(null, sql, params);
	}

	/**
	 * 执行含参数的sql语句
	 * 
	 * @param dbType
	 *            数据库类型oracle,sqlserver,mysql
	 * @param sql
	 *            被执行的sql语句
	 * @param params
	 *            参数数组
	 * @return 返回受影响的行
	 * @throws Exception
	 */
	public static int execute(String dbType, String sql, Object[] params) throws Exception {
		Connection conn = null;
		int rows = 0;
		try {
			if (dbType == null || dbType.length() == 0) {
				conn = ConnectionPool.getConnection();
			} else {
				conn = ConnectionPool.getConnection(dbType);
			}

			QueryRunner qr = new QueryRunner();
			rows = qr.update(conn, sql, params);
		} finally {
			ConnectionPool.close(conn);
		}
		return rows;
	}

	/**
	 * 执行含参数的sql语句
	 * 
	 * @param sql
	 *            被执行的sql语句
	 * @param params
	 *            可变参数
	 * @return 返回受影响的行
	 * @throws Exception
	 */
	public static int executeParam(String sql, Object... params) throws Exception {
		return executeParam(null, sql, params);
	}

	/**
	 * 执行含参数的sql语句
	 * 
	 * @param dbType
	 *            数据库类型oracle,sqlserver,mysql
	 * @param sql
	 *            被执行的sql语句
	 * @param params
	 *            可变参数
	 * @return 返回受影响的行
	 * @throws Exception
	 */
	public static int executeParam(String dbType, String sql, Object... params) throws Exception {
		Connection conn = null;
		int rows = 0;
		try {
			if (dbType == null || dbType.length() == 0) {
				conn = ConnectionPool.getConnection();
			} else {
				conn = ConnectionPool.getConnection(dbType);
			}

			QueryRunner qr = new QueryRunner();
			rows = qr.update(conn, sql, params);
		} finally {
			ConnectionPool.close(conn);
		}
		return rows;
	}

	/**
	 * 查询sql语句,返回1X1
	 * 
	 * @param sql
	 *            被执行的sql语句
	 * @return Object
	 * @throws Exception
	 */
	public static Object queryScalar(String sql) throws Exception {
		return queryScalar(null, sql);
	}

	/**
	 * 查询sql语句,返回1X1
	 * 
	 * @param dbType
	 *            数据库类型oracle,sqlserver,mysql
	 * @param sql
	 *            被执行的sql语句
	 * @return Object
	 * @throws Exception
	 */
	public static Object queryScalar(String dbType, String sql) throws Exception {
		Object val = null;

		ResultSet rs = null;
		Statement stmt = null;
		Connection conn = null;

		try {
			if (dbType == null || dbType.length() == 0) {
				conn = ConnectionPool.getConnection();
			} else {
				conn = ConnectionPool.getConnection(dbType);
			}
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);

			if (rs.next()) {
				val = rs.getObject(1);
			}

		} finally {
			ConnectionPool.close(rs, stmt, conn);
		}

		return val;
	}

	/**
	 * 根据参数查询sql语句,返回1X1
	 * 
	 * @param sql
	 *            sql语句
	 * @param param
	 *            参数
	 * @return Object
	 * @throws Exception
	 */
	public static Object queryScalar(String sql, Object param) throws Exception {
		return queryScalar(null, sql, param);
	}

	/**
	 * 根据参数查询sql语句,返回1X1
	 * 
	 * @param dbType
	 *            数据库类型oracle,sqlserver,mysql
	 * @param sql
	 *            sql语句
	 * @param param
	 *            参数
	 * @return Object
	 * @throws Exception
	 */
	public static Object queryScalar(String dbType, String sql, Object param) throws Exception {
		Object val = null;

		ResultSet rs = null;
		PreparedStatement pstm = null;
		Connection conn = null;

		try {
			if (dbType == null || dbType.length() == 0) {
				conn = ConnectionPool.getConnection();
			} else {
				conn = ConnectionPool.getConnection(dbType);
			}
			pstm = conn.prepareStatement(sql);
			pstm.setObject(1, param);
			rs = pstm.executeQuery();
			pstm.clearParameters();

			if (rs.next()) {
				val = rs.getObject(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(rs, pstm, conn);
		}
		return val;
	}

	/**
	 * 根据参数查询sql语句,返回1X1
	 * 
	 * @param sql
	 *            sql语句
	 * @param param
	 *            参数数组
	 * @return Object
	 * @throws Exception
	 */
	public static Object queryScalar(String sql, Object[] params) throws Exception {
		return queryScalar(null, sql, params);
	}

	/**
	 * 根据参数查询sql语句,返回1X1
	 * 
	 * @param dbType
	 *            数据库类型oracle,sqlserver,mysql
	 * @param sql
	 *            sql语句
	 * @param param
	 *            参数数组
	 * @return Object
	 * @throws Exception
	 */
	public static Object queryScalar(String dbType, String sql, Object[] params) throws Exception {
		Object val = null;

		ResultSet rs = null;
		PreparedStatement pstm = null;
		Connection conn = null;

		try {
			if (dbType == null || dbType.length() == 0) {
				conn = ConnectionPool.getConnection();
			} else {
				conn = ConnectionPool.getConnection(dbType);
			}
			pstm = conn.prepareStatement(sql);

			for (int i = 1; i <= params.length; i++) {
				pstm.setObject(i, params[i - 1]);
			}

			rs = pstm.executeQuery();
			pstm.clearParameters();

			if (rs.next()) {
				val = rs.getObject(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn);
		}
		return val;
	}

	/**
	 * 根据参数查询sql语句,返回1X1
	 * 
	 * @param sql
	 *            sql语句
	 * @param param
	 *            可变参数
	 * @return Object
	 * @throws Exception
	 */
	public static Object queryScalarParam(String sql, Object... params) throws Exception {
		return queryScalarParam(null, sql, params);
	}

	/**
	 * 根据参数查询sql语句,返回1X1
	 * 
	 * @param dbType
	 *            数据库类型oracle,sqlserver,mysql
	 * @param sql
	 *            sql语句
	 * @param param
	 *            可变参数
	 * @return Object
	 * @throws Exception
	 */
	public static Object queryScalarParam(String dbType, String sql, Object... params) throws Exception {
		Object val = null;

		ResultSet rs = null;
		PreparedStatement pstm = null;
		Connection conn = null;

		try {
			if (dbType == null || dbType.length() == 0) {
				conn = ConnectionPool.getConnection();
			} else {
				conn = ConnectionPool.getConnection(dbType);
			}
			pstm = conn.prepareStatement(sql);

			for (int i = 1; i <= params.length; i++) {
				pstm.setObject(i, params[i - 1]);
			}

			rs = pstm.executeQuery();
			pstm.clearParameters();

			if (rs.next()) {
				val = rs.getObject(1);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn);
		}
		return val;
	}

	/**
	 * 查询sql语句。
	 * 
	 * @param sql
	 *            被执行的sql语句
	 * @return List<Map<String,Object>>
	 * @throws Exception
	 */
	public static List<Map<String, Object>> query(String sql) throws Exception {
		return query(null, sql);
	}

	/**
	 * 查询sql语句。
	 * 
	 * @param dbType
	 *            数据库类型oracle,sqlserver,mysql
	 * @param sql
	 *            被执行的sql语句
	 * @return List<Map<String,Object>>
	 * @throws Exception
	 */
	public static List<Map<String, Object>> query(String dbType, String sql) throws Exception {
		List<Map<String, Object>> results = null;

		Connection conn = null;
		try {
			if (dbType == null || dbType.length() == 0) {
				conn = ConnectionPool.getConnection();
			} else {
				conn = ConnectionPool.getConnection(dbType);
			}
			QueryRunner qr = new QueryRunner();

			results = qr.query(conn, sql, new MapListHandler());
		} finally {
			ConnectionPool.close(conn);
		}
		return results;
	}

	/**
	 * 根据参数查询sql语句
	 * 
	 * @param sql
	 *            sql语句
	 * @param param
	 *            参数
	 * @return List<Map<String,Object>>
	 * @throws Exception
	 */
	public static List<Map<String, Object>> query(String sql, Object param) throws Exception {
		return query(null, sql, param);
	}

	/**
	 * 根据参数查询sql语句
	 * 
	 * @param dbType
	 *            数据库类型oracle,sqlserver,mysql
	 * @param sql
	 *            sql语句
	 * @param param
	 *            参数
	 * @return List<Map<String,Object>>
	 * @throws Exception
	 */
	public static List<Map<String, Object>> query(String dbType, String sql, Object param) throws Exception {
		List<Map<String, Object>> results = null;
		Connection conn = null;

		try {
			if (dbType == null || dbType.length() == 0) {
				conn = ConnectionPool.getConnection();
			} else {
				conn = ConnectionPool.getConnection(dbType);
			}
			QueryRunner qr = new QueryRunner();

			results = (List<Map<String, Object>>) qr.query(conn, sql, new MapListHandler(), param);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn);
		}
		return results;
	}

	/**
	 * 根据参数查询sql语句
	 * 
	 * @param sql
	 *            sql语句
	 * @param param
	 *            参数数组
	 * @return List<Map<String,Object>>
	 * @throws Exception
	 */
	public static List<Map<String, Object>> query(String sql, Object[] params) throws Exception {
		return query(null, sql, params);
	}

	/**
	 * 根据参数查询sql语句
	 * 
	 * @param dbType
	 *            数据库类型oracle,sqlserver,mysql
	 * @param sql
	 *            sql语句
	 * @param param
	 *            参数数组
	 * @return List<Map<String,Object>>
	 * @throws Exception
	 */
	public static List<Map<String, Object>> query(String dbType, String sql, Object[] params) throws Exception {
		List<Map<String, Object>> results = null;
		Connection conn = null;

		try {
			if (dbType == null || dbType.length() == 0) {
				conn = ConnectionPool.getConnection();
			} else {
				conn = ConnectionPool.getConnection(dbType);
			}
			QueryRunner qr = new QueryRunner();

			results = (List<Map<String, Object>>) qr.query(conn, sql, new MapListHandler(), params);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn);
		}
		return results;
	}

	/**
	 * 根据参数查询sql语句
	 * 
	 * @param sql
	 *            sql语句
	 * @param param
	 *            可变参数
	 * @return List<Map<String,Object>>
	 * @throws Exception
	 */
	public static List<Map<String, Object>> queryParam(String sql, Object... params) throws Exception {
		return queryParam(null, sql, params);
	}

	/**
	 * 根据参数查询sql语句
	 * 
	 * @param dbType
	 *            数据库类型oracle,sqlserver,mysql
	 * @param sql
	 *            sql语句
	 * @param param
	 *            可变参数
	 * @return List<Map<String,Object>>
	 * @throws Exception
	 */
	public static List<Map<String, Object>> queryParam(String dbType, String sql, Object... params) throws Exception {
		List<Map<String, Object>> results = null;
		Connection conn = null;

		try {
			if (dbType == null || dbType.length() == 0) {
				conn = ConnectionPool.getConnection();
			} else {
				conn = ConnectionPool.getConnection(dbType);
			}
			QueryRunner qr = new QueryRunner();

			results = (List<Map<String, Object>>) qr.query(conn, sql, new MapListHandler(), params);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn);
		}
		return results;
	}

	/**
	 * 功能：ResultSet 转为List<Map<String,Object>>
	 * 
	 * 
	 * @param rs
	 *            ResultSet 原始数据集
	 * @return List<Map<String,Object>>
	 * @throws java.sql.SQLException
	 */
	private static List<Map<String, Object>> resultSetToList(ResultSet rs) throws java.sql.SQLException {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

		if (rs != null) {
			// 得到结果集(rs)的结构信息，比如字段数、字段名等
			ResultSetMetaData md = rs.getMetaData();
			// 返回此 ResultSet 对象中的列数
			int columnCount = md.getColumnCount();

			Map<String, Object> rowData = new HashMap<String, Object>();
			while (rs.next()) {
				rowData = new HashMap<String, Object>(columnCount);

				for (int i = 1; i <= columnCount; i++) {
					rowData.put(md.getColumnName(i), rs.getObject(i));
				}

				list.add(rowData);
			}

			rs.close();
		}

		return list;
	}

}
