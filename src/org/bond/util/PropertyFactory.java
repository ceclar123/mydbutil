package org.bond.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingConnection;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.KeyedObjectPoolFactory;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPoolFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

public class PropertyFactory {
	public static final String defaultDataSourceName = "default";
	private static final String defaultFileName = "datasource.properties";
	protected Map<String, Properties> datasourceParams = new HashMap<String, Properties>();
	protected Map<String, DataSource> datasourceMap = new HashMap<String, DataSource>();

	/**
	 * 构造函数
	 * 
	 * @throws RuntimeException
	 *             If the datasource.properties could not be found or property
	 *             key is illegal.
	 * @throws IOException
	 *             If could not load properties from datasource.properties
	 */
	public PropertyFactory() throws IOException {
		this(defaultFileName);
	}

	/**
	 * 构造函数
	 * 
	 * @param fileName
	 *            The resource name
	 * @throws RuntimeException
	 *             If the resource could not be found or property key is
	 *             illegal.
	 * @throws IOException
	 *             If could not load properties from resource
	 */
	public PropertyFactory(String fileName) throws IOException {
		InputStream in = PropertyFactory.class.getClassLoader().getResourceAsStream(fileName);
		if (in == null) {
			throw new RuntimeException("The [" + fileName + "] is not found.");
		}
		// load properties from stream
		Properties props = new Properties();
		try {
			props.load(in);
		} catch (IOException e) {
			throw new IOException("Could not load [" + fileName + "] config file.", e);
		}
		// analyze properties
		Iterator<Object> it = props.keySet().iterator();
		while (it.hasNext()) {
			String key = (String) it.next();
			String value = props.getProperty(key);
			int dotIndex = key.indexOf('.');
			if (dotIndex < 1) {
				// dotIndex must > 1
				// [default.driver=oracle.jdbc.driver.OracleDriver]
				throw new RuntimeException("Illegal property key [" + key + "].");
			}

			String datasourceName = key.substring(0, dotIndex);
			key = key.substring(dotIndex + 1);
			Properties params = datasourceParams.get(datasourceName);
			if (params == null) {
				params = new Properties();
				datasourceParams.put(datasourceName, params);
			}
			params.put(key, value);
		}
	}

	/**
	 * 返回默认数据源
	 * 
	 * @return dataSource or null
	 * @throws Exception
	 */
	public DataSource getDataSource() throws Exception {
		return getDataSource("default");
	}

	/**
	 * 返回指定数据源
	 * 
	 * @param dataSourceName
	 *            The dataSource name
	 * @return dataSource or null
	 * @throws Exception
	 */
	public DataSource getDataSource(String dataSourceName) throws Exception {
		DataSource datasource = datasourceMap.get(dataSourceName);
		if (datasource != null) {
			return datasource;
		}
		datasource = createDataSource(datasourceParams.get(dataSourceName));
		if (datasource != null) {
			datasourceMap.put(dataSourceName, datasource);
		}
		return datasource;
	}

	/**
	 * 创建一个数据源
	 * 
	 * @param props
	 * @return dataSource or null
	 * @throws IllegalArgumentException
	 *             If props is null
	 */
	protected DataSource createDataSource(Properties props) {
		if (props == null) {
			throw new IllegalArgumentException("props can not be null.");
		}
		if (props.getProperty("jndiName") != null) {
			DataSource jndiDataSource = getJndiDataSource(props);
			if (jndiDataSource != null) {
				return jndiDataSource;
			}
		}

		BasicDataSource dataSource = new BasicDataSource();

		// required
		dataSource.setDriverClassName(props.getProperty("driverClassName"));
		dataSource.setUrl(props.getProperty("url"));
		dataSource.setUsername(props.getProperty("username", ""));
		dataSource.setPassword(props.getProperty("password", ""));
		// options
		dataSource.setInitialSize(Integer.parseInt(props.getProperty("initialSize", "3")));
		dataSource.setMaxActive(Integer.parseInt(props.getProperty("maxActive", "10")));
		dataSource.setMaxIdle(Integer.parseInt(props.getProperty("maxIdle", "5")));
		dataSource.setMaxWait(Long.parseLong(props.getProperty("maxWait", "-1")));
		dataSource.setValidationQuery(props.getProperty("validationQuery"));
		dataSource.setTestOnBorrow(Boolean.valueOf(props.getProperty("testOnBorrow", "true")));
		dataSource.setTestOnReturn(Boolean.valueOf(props.getProperty("testOnReturn", "true")));
		dataSource.setTestWhileIdle(Boolean.valueOf(props.getProperty("testWhilwIdle", "true")));
		dataSource.setNumTestsPerEvictionRun(Integer.valueOf(props.getProperty("numTestsPerEvictionRun", "3")));
		dataSource.setMinEvictableIdleTimeMillis(Long.parseLong(props.getProperty("minEvictableIdleTimeMillis", "600000")));
		dataSource.setTimeBetweenEvictionRunsMillis(Long.parseLong(props.getProperty("timeBetweenEvictionRunsMillis", "-1")));

		return dataSource;
	}

	/**
	 * 创建一个数据源
	 * 
	 * @param props
	 * @param pool
	 * @return dataSource or null
	 * @throws IllegalArgumentException
	 *             If props is null
	 */
	protected DataSource createDataSource(Properties props, boolean pool) throws ClassNotFoundException {
		if (props == null) {
			throw new IllegalArgumentException("properties can not be null.");
		}

		Class.forName(props.getProperty("driverClassName"));

		ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(props.getProperty("url"), props.getProperty("username", ""), props.getProperty("password", ""));

		GenericObjectPool<Object> connectionPool = new GenericObjectPool<Object>();
		KeyedObjectPoolFactory<Object, Object> stmtPool = new GenericKeyedObjectPoolFactory<Object, Object>(null);
		PoolableConnectionFactory poolConnnectionFactory = new PoolableConnectionFactory(connectionFactory, connectionPool, stmtPool, null, false, true);

		DataSource dataSource = new PoolingDataSource(connectionPool);

		connectionPool.setMaxActive(Integer.valueOf(props.getProperty("maxActive", "10")));
		connectionPool.setMaxIdle(Integer.parseInt(props.getProperty("maxIdle", "5")));
		connectionPool.setMinIdle(Integer.valueOf(props.getProperty("maxIdle", "3")));
		connectionPool.setMaxWait(Long.parseLong(props.getProperty("maxWait", "-1")));
		connectionPool.setTestOnBorrow(Boolean.valueOf(props.getProperty("testOnBorrow", "true")));
		connectionPool.setTestOnReturn(Boolean.valueOf(props.getProperty("testOnReturn", "true")));
		connectionPool.setTestWhileIdle(Boolean.valueOf(props.getProperty("testWhilwIdle", "true")));

		connectionPool.setNumTestsPerEvictionRun(Integer.valueOf(props.getProperty("numTestsPerEvictionRun", "3")));
		connectionPool.setMinEvictableIdleTimeMillis(Long.parseLong(props.getProperty("minEvictableIdleTimeMillis", "600000")));
		connectionPool.setSoftMinEvictableIdleTimeMillis(Long.parseLong(props.getProperty("softMinEvictableIdleTimeMillis", "-1")));
		connectionPool.setTimeBetweenEvictionRunsMillis(Long.parseLong(props.getProperty("timeBetweenEvictionRunsMillis", "-1")));

		return dataSource;
	}

	/**
	 * 返回 jndi 数据源
	 * 
	 * @param props
	 * @return dataSource or null
	 */
	protected DataSource getJndiDataSource(Properties props) {
		String jndiName = props.getProperty("jndiName");
		String inContainer = props.getProperty("inContainer");
		String prefix = "java:comp/env/";
		if (!jndiName.startsWith(prefix) && "true".equals(inContainer)) {
			jndiName = prefix + jndiName;
		}
		//
		DataSource jndiDataSource = null;
		Context context = null;
		try {
			context = new InitialContext(props);
			jndiDataSource = (DataSource) context.lookup(jndiName);
			if (jndiDataSource != null) {
				return jndiDataSource;
			}
		} catch (NamingException e) {
			// ignore
		} finally {
			try {
				if (context != null) {
					context.close();
				}
			} catch (NamingException e) {
				// ignore
			}
		}
		return jndiDataSource;
	}
}
