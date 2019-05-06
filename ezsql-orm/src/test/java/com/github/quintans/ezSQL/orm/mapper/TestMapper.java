package com.github.quintans.ezSQL.orm.mapper;

import com.github.quintans.ezSQL.TransactionManager;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.orm.Db;
import com.github.quintans.ezSQL.orm.app.domain.ArtistVO;
import com.github.quintans.ezSQL.orm.app.mappings.TArtist;
import com.github.quintans.ezSQL.orm.app.mappings.TPainting;
import com.github.quintans.ezSQL.orm.extended.H2DriverExt;
import org.h2.jdbcx.JdbcConnectionPool;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestMapper {
    protected static TransactionManager<com.github.quintans.ezSQL.orm.Db> tm;

    @BeforeClass
    public static void testSetup() throws Exception {
        try {
            JdbcConnectionPool cp = JdbcConnectionPool.create(
                    "jdbc:h2:mem:mapper", "sa", "");

            Connection conn = cp.getConnection();
            conn.createStatement().execute("CREATE TABLE ARTIST (\n" +
                    "\tID BIGINT IDENTITY,\n" +
                    "\tVERSION INT,\n" +
                    "\tNAME VARCHAR(255) NOT NULL,\n" +
                    "\tCREATION TIMESTAMP,\n" +
                    ");\n");
            conn.createStatement().execute("CREATE TABLE PAINTING (\n" +
                    "\tID BIGINT IDENTITY,\n" +
                    "\tVERSION INT,\n" +
                    "\tNAME VARCHAR(255) NOT NULL,\n" +
                    "\tPRICE DOUBLE NOT NULL,\n" +
                    "\tARTIST_ID BIGINT NOT NULL,\n" +
                    "\tCREATION TIMESTAMP,\n" +
                    ");\n");
            conn.close();

            Driver driver = new H2DriverExt();
            tm = new TransactionManager<>(
                    () -> {
                        Db db = new Db(driver, cp.getConnection());
                        db.registerQueryMappers(
                                BuilderMapper.class,
                                MapMapper.class
                        );
                        return db;
                    }
            );

            tm.transactionNoResult(db -> {
                db.insert(TArtist.T_ARTIST)
                        .set(TArtist.C_ID, 1L)
                        .set(TArtist.C_NAME, "Unknown")
                        .execute();
                db.insert(TPainting.T_PAINTING)
                        .set(TPainting.C_ID, 1L)
                        .set(TPainting.C_NAME, "White Noise")
                        .set(TPainting.C_PRICE, 130_000D)
                        .set(TPainting.C_ARTIST, 1L)
                        .execute();
                db.insert(TPainting.T_PAINTING)
                        .set(TPainting.C_ID, 2L)
                        .set(TPainting.C_NAME, "Pitch Black")
                        .set(TPainting.C_PRICE, 100_000D)
                        .set(TPainting.C_ARTIST, 1L)
                        .execute();
            });

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void givenMapperWhenQueryThenReturnArtistVO() {
        tm.transactionNoResult(db -> {
            List<ArtistVO> result = db.query(TArtist.T_ARTIST).column(TArtist.C_ID, TArtist.C_NAME)
                    .list(ArtistVO.class, false);

            System.out.println(result);
            assertEquals("Name of Artist was null.", "Unknown", result.get(0).getName());
        });
    }

    @Test
    public void givenMapMapperWhenQueryThenReturnMap() {
        tm.transactionNoResult(db -> {
            List<HashMap> result = db.query(TArtist.T_ARTIST).column(TArtist.C_ID, TArtist.C_NAME)
                    .list(HashMap.class, false);

            System.out.println(result);
            assertEquals("Name of Artist was null.", "Unknown", result.get(0).get("name"));
        });
    }

    @Ignore
    @Test
    public void givenMapperWhenQueryThenReturnMap() {
        tm.transactionNoResult(db -> {
            List<ArtistVO> result = db.query(TArtist.T_ARTIST).column(TArtist.C_ID, TArtist.C_NAME)
                    .outer(TArtist.A_PAINTINGS)
                    .include(TPainting.C_ID, TPainting.C_NAME, TPainting.C_PRICE)
                    .join()
                    .list(ArtistVO.class, false);

            assertEquals("Name of Artist was null.", "Unknown", result.get(0).getName());
        });
    }
}
