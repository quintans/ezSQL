package com.github.quintans.ezSQL;

import com.github.quintans.ezSQL.TransactionManager;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.orm.Db;
import com.github.quintans.ezSQL.orm.TestBootstrap;
import com.github.quintans.ezSQL.orm.app.mappings.TArtist;
import com.github.quintans.ezSQL.orm.app.mappings.TPainting;
import com.github.quintans.ezSQL.orm.extended.H2DriverExt;
import com.github.quintans.ezSQL.orm.mapper.MapMapper;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestMapper extends TestBootstrap {
    protected static TransactionManager<com.github.quintans.ezSQL.orm.Db> tm;

    @BeforeClass
    public static void testSetup() throws Exception {
        try {
            JdbcConnectionPool cp = JdbcConnectionPool.create(
                    "jdbc:h2:mem:test", "sa", "");

            Connection conn = cp.getConnection();
            conn.createStatement().execute("CREATE TABLE ARTIST (\n" +
                    "\tID BIGINT IDENTITY,\n" +
                    "\tNAME VARCHAR(255) NOT NULL\n" +
                    ");\n");
            conn.createStatement().execute("CREATE TABLE PAINTING (\n" +
                    "\tID BIGINT IDENTITY,\n" +
                    "\tNAME VARCHAR(255) NOT NULL,\n" +
                    "\tARTIST_ID BIGINT NOT NULL\n" +
                    ");\n");
            conn.close();

            Driver driver = new H2DriverExt();
            tm = new TransactionManager<>(
                    () -> new Db(driver, cp.getConnection())
            );

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }


    @Test
    public void givenMapperWhenQueryThenReturnMap() {
        tm.transactionNoResult(db -> {
            db.insert(TArtist.T_ARTIST)
                    .set(TArtist.C_ID, 1L)
                    .set(TArtist.C_NAME, "Unknown")
                    .execute();
            db.insert(TPainting.T_PAINTING)
                    .set(TPainting.C_ID, 1L)
                    .set(TPainting.C_NAME, "White Noise")
                    .set(TPainting.C_ARTIST, 1L)
                    .execute();
            db.insert(TPainting.T_PAINTING)
                    .set(TPainting.C_ID, 2L)
                    .set(TPainting.C_NAME, "Pitch Black")
                    .set(TPainting.C_ARTIST, 1L)
                    .execute();

            List<Map<String, Object>> result = db.query(TArtist.T_ARTIST).column(TArtist.C_ID, TArtist.C_NAME)
                    .outerFetch(TArtist.A_PAINTINGS)
                    .column(TPainting.C_ID, TPainting.C_NAME, TPainting.C_PRICE)
                    .list(new MapMapper<>(), false);

            assertEquals("Name of Artist was null.", "Unknown", result.get(0).get("NAME"));
        });
    }
}
