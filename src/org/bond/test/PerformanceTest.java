package org.bond.test;

import org.bond.util.DBUtil;

public class PerformanceTest implements Runnable {
	private static int iCount = 0;
	static long start = 0l;
	static long end = 0l;

	@Override
	public void run() {
		try {
			for (int i = 0; i < 10; i++) {
				String sql = "update dept set dname=? where deptno=?";
				DBUtil.executeParam("oracle", sql, "测试部门", 20);
				sql = "select dname from dept where deptno=?";
				Object val = DBUtil.queryScalar("oracle", sql, 20);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		iCount++;

		if (iCount == 1) {
			start = System.currentTimeMillis();
		}
		if (iCount == 80) {
			end = System.currentTimeMillis();

			long minute = (end - start) / 6000;
			long second = ((end - start) - minute * 6000) / 1000;
			long minsecond = (end - start) - 6000 * minute - second * 1000;

			System.out.println("------------耗时:" + minute + "分" + second + "秒" + minsecond + "毫秒-------------");
		}
	}
}
