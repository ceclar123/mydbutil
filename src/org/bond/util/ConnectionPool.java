package org.bond.util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

public class ConnectionPool {
	protected static DataSource dataSource = null;

	protected static PropertyFactory _factory;
	static {
		try {
			_factory = new PropertyFactory();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	synchronized protected static void initDataSource(String dataSourceName) throws Exception {
		dataSource = _factory.getDataSource(dataSourceName);
	}

	/**
	 * 返回默认数据源的连接
	 * 
	 * @return connection or null
	 * @throws Exception
	 */
	public static Connection getConnection() throws Exception {
		return getConnection(PropertyFactory.defaultDataSourceName);
	}

	/**
	 * 返回指定数据源的连接
	 * 
	 * @param dataSourceName
	 *            The dataSource name
	 * @return connection or null
	 * @throws Exception
	 */
	public static Connection getConnection(String dataSourceName) throws Exception {
		if (dataSource == null) {
			initDataSource(dataSourceName);
		}

		Connection conn = null;
		try {
			conn = dataSource.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return conn;
	}

	/**
	 * 关闭连接对象
	 *
	 * @param conn
	 *            Connection
	 */
	public static void close(Connection conn) {
		try {
			if (conn != null) {
				conn.close();
			}
		} catch (Exception e) {
			//
		}
	}

	/**
	 * 关闭连接对象
	 *
	 * @param stmt
	 *            Statement
	 * @param conn
	 *            Connection
	 */
	public static void close(Statement stmt, Connection conn) {
		try {
			if (stmt != null) {
				stmt.close();
			}
		} catch (Exception e) {
			//
		}
		try {
			if (conn != null) {
				conn.close();
			}
		} catch (Exception e) {
			//
		}
	}

	/**
	 * 关闭连接对象
	 *
	 * @param pstm
	 *            PreparedStatement
	 * @param conn
	 *            Connection
	 */
	public static void close(PreparedStatement pstm, Connection conn) {
		try {
			if (pstm != null) {
				pstm.close();
			}
		} catch (Exception e) {
			//
		}
		try {
			if (conn != null) {
				conn.close();
			}
		} catch (Exception e) {
			//
		}
	}

	/**
	 * 关闭连接对象
	 * 
	 * @param rset
	 *            ResultSet
	 * @param stmt
	 *            Statement
	 * @param conn
	 *            Connection
	 */
	public static void close(ResultSet rset, Statement stmt, Connection conn) {
		try {
			if (rset != null) {
				rset.close();
			}
		} catch (Exception e) {
			//
		}
		try {
			if (stmt != null) {
				stmt.close();
			}
		} catch (Exception e) {
			//
		}
		try {
			if (conn != null) {
				conn.close();
			}
		} catch (Exception e) {
			//
		}
	}
}
