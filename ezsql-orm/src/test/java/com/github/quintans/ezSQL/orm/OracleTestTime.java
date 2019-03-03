package com.github.quintans.ezSQL.orm;

import static com.github.quintans.ezSQL.dml.Definition.raw;

import java.sql.Connection;
import java.util.Collection;
import java.util.Date;

import com.github.quintans.ezSQL.TransactionManager;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;

import com.github.quintans.ezSQL.common.type.MyDateTime;
import com.github.quintans.ezSQL.dml.Query;
import com.github.quintans.ezSQL.dml.Update;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.driver.Oracle5Driver;
import com.github.quintans.ezSQL.orm.app.mappings.TTimer;

/**
 * Unit test for simple App.
 */
public class OracleTestTime extends TestCase {
	private IDatabaseTester databaseTester;

	private Driver driver;
	private TransactionManager<Db> tm;

	/**
	 * Create the test case
	 * 
	 * @param testName
	 *            name of the test case
	 */
	public OracleTestTime(String testName) {
		super(testName);
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite() {
		return new TestSuite(OracleTestTime.class);
	}

	@Override
	protected void setUp() throws Exception {
		super.setUp();

		this.databaseTester = new JdbcDatabaseTester("oracle.jdbc.driver.OracleDriver", "jdbc:oracle:thin:@vmdb:1521:XE", "LIXO", "LIXO");

		Connection conn = this.databaseTester.getConnection().getConnection();
		Oracle5Driver drv = new Oracle5Driver();
		drv.setTimeZoneId("GMT");
		System.out.println("tz: " + drv.getCalendar().getTimeZone().getID());
		this.driver = drv;

		tm = new TransactionManager<Db>(
				() -> conn,
				c -> new Db(this.driver, c)
		) {
			@Override
			protected void close(Connection con) {
				// no-op
			}
		};
	}

	@Override
	protected void tearDown() throws Exception {
		this.databaseTester.onTearDown();
	}

	public void testUpdate() {
		tm.transactionNoResult(db -> {
			long ticks = 1307882799572L;
			Update upd = new Update(db, TTimer.T_TIMER)
					.set(TTimer.C_DATE, new MyDateTime(ticks))
					.set(TTimer.C_TIMESTAMP, new Date(ticks))
					.where(TTimer.C_ID.is(raw(1L)));

			upd.execute();
		});
	}

	public void testSelect() {
		tm.readOnlyNoResult(db -> {
			Query query = db.queryAll(TTimer.T_TIMER);
			Collection<Object[]> list = query.listRaw();
			for (Object o : list) {
				Object[] objs = (Object[]) o;
				System.out.print(objs[0] + " | ");
				System.out.print(objs[1] + " | ");
				System.out.println(objs[2]);
			}

			assertTrue(list.size() > 0);
		});
	}
}
