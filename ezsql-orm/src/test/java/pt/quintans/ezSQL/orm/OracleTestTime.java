package pt.quintans.ezSQL.orm;

import static pt.quintans.ezSQL.dml.Definition.raw;

import java.util.Collection;
import java.util.Date;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;

import pt.quintans.ezSQL.common.type.MyDateTime;
import pt.quintans.ezSQL.dml.Query;
import pt.quintans.ezSQL.dml.Update;
import pt.quintans.ezSQL.driver.Driver;
import pt.quintans.ezSQL.driver.Oracle5Driver;
import pt.quintans.ezSQL.orm.app.mappings.TTimer;

/**
 * Unit test for simple App.
 */
public class OracleTestTime extends TestCase {
	private IDatabaseTester databaseTester;

	private Db db;
	private Driver driver;

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

		this.db = new Db(this.databaseTester.getConnection().getConnection());
		Oracle5Driver drv = new Oracle5Driver();
		drv.setTimeZoneId("GMT");
		System.out.println("tz: " + drv.getCalendar().getTimeZone().getID());
		this.driver = drv;
		this.db.setDriver(this.driver);
	}

	@Override
	protected void tearDown() throws Exception {
		this.databaseTester.onTearDown();
	}

	public void testUpdate() {
		try {
		    long ticks = 1307882799572L;
			Update upd = new Update(this.db, TTimer.T_TIMER)
				.set(TTimer.C_DATE, new MyDateTime(ticks))
				.set(TTimer.C_TIMESTAMP, new Date(ticks))
				.where(TTimer.C_ID.is(raw(1L)));


			upd.execute();

			this.db.getConnection().commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void testSelect() {
		try {
			Query query = this.db.queryAll(TTimer.T_TIMER);
			Collection<Object[]> list = query.listRaw();
			for (Object o : list) {
				Object[] objs = (Object[]) o;
				System.out.print(objs[0] + " | ");
				System.out.print(objs[1] + " | ");
				System.out.println(objs[2]);
			}

			assertTrue(list.size() > 0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
