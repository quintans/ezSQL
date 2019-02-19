package com.github.quintans.ezSQL;

import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.orm.Db;
import com.github.quintans.ezSQL.orm.app.mappings.discriminator.TAa;
import com.github.quintans.ezSQL.orm.extended.H2DriverExt;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;

import static com.github.quintans.ezSQL.orm.app.mappings.discriminator.TAa.T_TAA;
import static org.junit.Assert.assertTrue;

public class TransactionTest {
    protected static TransactionManager<com.github.quintans.ezSQL.orm.Db> tm;

    @BeforeClass
    public static void testSetup() throws Exception {
        try {
            JdbcConnectionPool cp = JdbcConnectionPool.create(
                    "jdbc:h2:mem:test", "sa", "");

            Connection conn = cp.getConnection();
            conn.createStatement().execute("CREATE TABLE TA (\n" +
                    "\tID BIGINT IDENTITY,\n" +
                    "\tTIPO VARCHAR(255)\n" +
                    ");\n");
            conn.close();

            Driver driver = new H2DriverExt();
            tm = new TransactionManager<>(
                    () -> cp.getConnection(),
                    c -> new Db(c, driver)
            );

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Before
    public void setup() {
        tm.transaction(db -> {
            db.delete(T_TAA).execute();
        });
    }

    private void populateTAa(Db db) {
        db.insert(TAa.T_TAA)
                .set(TAa.C_ID, 1L)
                .set(TAa.C_TYPE, "xxx")
                .execute();
    }

    private List<TAa> listTAa(Db db) {
        return db.query(T_TAA).all().list(TAa.class);
    }

    @Test
    public void testHasReadOnlyTransactions() {
        tm.readOnly(db -> {
            Connection conn = db.getConnection();
            assertTrue("No readonly transaction", conn != null);

            populateTAa(db);
        });
        tm.readOnly(db -> {
            List<TAa> list = listTAa(db);
            assertTrue("List should be empty.", list.isEmpty());
        });
    }

    @Test
    public void testHasRequiredTransactions() {
        tm.transaction(db -> {
            Connection conn = db.getConnection();
            assertTrue("No transaction transaction", conn != null);

            populateTAa(db);
        });
        tm.readOnly(db -> {
            List<TAa> list = listTAa(db);
            assertTrue("Expected size 1, got" + list.size(), list.size() == 1);
        });
    }

    @Test(expected = PersistenceException.class)
    public void testHasRequiredNewTransactions() {
        tm.transaction(db -> {
            Connection conn = db.getConnection();
            assertTrue("No transaction new transaction", conn != null);
            populateTAa(db);
            throw new PersistenceException("Rollback");
        });

        tm.readOnly(db -> {
            List<TAa> list = listTAa(db);
            assertTrue("List should be empty.", list.isEmpty());
        });
    }

    @Test
    public void testHasNewInnerTransactions() {
        tm.transaction(db1 -> {
            Connection conn1 = db1.getConnection();

            tm.transaction(db2 -> {
                Connection conn2 = db2.getConnection();
                assertTrue("No transaction new inner transaction", conn2 != null && conn1 != null && conn1 != conn2);
            });
            assertTrue("Connection is closed", conn1 != null && !conn1.isClosed());
        });
    }

}
