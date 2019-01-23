package com.github.quintans.ezSQL.orm;

import com.github.quintans.ezSQL.common.type.MyDate;
import com.github.quintans.ezSQL.common.type.MyDateTime;
import com.github.quintans.ezSQL.common.type.MyTime;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.dml.Delete;
import com.github.quintans.ezSQL.dml.Insert;
import com.github.quintans.ezSQL.dml.Query;
import com.github.quintans.ezSQL.dml.Update;
import com.github.quintans.ezSQL.exceptions.OptimisticLockException;
import com.github.quintans.ezSQL.orm.app.daos.ArtistDAOBase;
import com.github.quintans.ezSQL.orm.app.daos.PaintingDAOBase;
import com.github.quintans.ezSQL.orm.app.domain.*;
import com.github.quintans.ezSQL.orm.app.dtos.ArtistValueDTO;
import com.github.quintans.ezSQL.orm.app.dtos.ImageDTO;
import com.github.quintans.ezSQL.orm.app.mappings.*;
import com.github.quintans.ezSQL.orm.extended.FunctionExt;
import com.github.quintans.ezSQL.toolkit.io.BinStore;
import com.github.quintans.ezSQL.transformers.MapBeanTransformer;
import com.github.quintans.ezSQL.transformers.SimpleAbstractDbRowTransformer;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

import static com.github.quintans.ezSQL.dml.Definition.*;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for simple App.
 */
public class TestStandard extends TestBootstrap {
    private static final long YEAR = 365L * 24L * 3600000L;

    @Test
    public void testLoadAssociation() {
        Artist artist = db.query(TArtist.T_ARTIST).all()
                .where(TArtist.C_ID.is(1L))
                .select(Artist.class);
        Set<Painting> paintings = db.loadAssociation(artist, TArtist.A_PAINTINGS);
        dumpCollection(paintings);
        dump(artist);

        assertTrue("Wrong set size when loading association paintings of Artist.", paintings != null && paintings.size() == 2);
    }

    @Test
    public void testEnum() {
        Query query = db.query(TArtist.T_ARTIST).column(TArtist.C_GENDER);
        List<EGender> genders = query.listRaw(EGender.class);
        dumpCollection(genders);

        genders = query.listRaw(EGender.class);
        dumpCollection(genders);

        assertTrue("Wrong size for gender list.", genders.size() == 3);
        assertTrue("Gender are incorrect null.",
                genders.get(0) == EGender.MALE &&
                        genders.get(1) == EGender.MALE &&
                        genders.get(2) == EGender.FEMALE
        );
    }

    @Test
    public void testAnd() {
        List<Artist> artists = db.queryAll(TArtist.T_ARTIST)
                .where(
                        TArtist.C_NAME.like("%n%").and(TArtist.C_GENDER.is(EGender.FEMALE))
                )
                .list(Artist.class);
        dumpCollection(artists);

        assertTrue("Wrong list size when testing Artist AND! expected 2!" + artists.size(), artists.size() == 1);
    }

    @Test
    public void testOr() {
        List<Artist> artists = db.queryAll(TArtist.T_ARTIST)
                .where(
                        TArtist.C_NAME.like("J%").or(TArtist.C_GENDER.is(EGender.MALE))
                )
                .list(Artist.class);
        dumpCollection(artists);

        assertTrue("Wrong list size when testing Artist AND! expected 3!" + artists.size(), artists.size() == 3);
    }

    @Test
    public void testCyclicFkReference() {
        Query query = db.queryAll(TArtist.T_ARTIST).innerFetch(TArtist.A_PAINTINGS);
        List<Artist> artists = query.list(ArtistDAOBase.factory);
        dumpCollection(artists);

        assertTrue("Wrong size for artist list.", artists.size() == 2);
    }

    @Test
    public void testCyclicFkReference2() {
        Query query = db.queryAll(TPainting.T_PAINTING).innerFetch(TPainting.A_ARTIST);
        List<Painting> entities = query.list(PaintingDAOBase.factory);
        dumpCollection(entities);

        assertTrue("Wrong size for artist list.", entities.size() == 4);
    }

    @Test
    public void testListRaw() {

        Stopwatch sw = Stopwatch.createAndStart();

        Query query = db.query(TArtist.T_ARTIST)
                .column(TArtist.C_ID)
                .column(TArtist.C_NAME);
        List<Object[]> list = query.listRaw(Long.class, String.class);

        sw.showTotal(null);

        dumpRaw(list);

        assertTrue(list.size() > 0);
    }

    @Test
    public void testSimpleTransformer() throws Exception {
        try {
            Query query = db.query(TArtist.T_ARTIST)
                    .column(TArtist.C_ID)
                    .column(TArtist.C_NAME)
                    .column(TArtist.C_GENDER);

            List<Artist> values = query.list(new SimpleAbstractDbRowTransformer<Artist>(db) {
                @Override
                public Artist transform(ResultSetWrapper rsw) throws SQLException {
                    Artist dto = new Artist();
                    dto.setId(toLong(rsw, 1));
                    dto.setName(toString(rsw, 2));
                    dto.setGender(driver().fromDb(rsw, 3, EGender.class));
                    return dto;
                }
            });
            dumpCollection(values);

            assertTrue("Wrong size for artist list.", values.size() == 3);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testSimpleBeanTransformer() throws Exception {
        try {
            Query query = db.queryAll(TArtist.T_ARTIST).order(TArtist.C_NAME).desc();
            // List<Artist> values = db.select(new
            // BeanTransformer<Artist>(query, Artist.class)); // long version
            // query.setFirstResult(10);
            query.skip(1).limit(1);
            List<Artist> values = query.list(Artist.class);
            dumpCollection(values);

            assertTrue("List returned form list is incorrect!", values.size() == 1);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testSubQueryInWhere() throws Exception {
        try {
            Query subquery = db.query(TPainting.T_PAINTING).as("t1")
                    .distinct()
                    .column(TPainting.C_ARTIST)
                    .where(TPainting.C_PRICE.gtoe(param("price")));

            Query query = db.query(TArtist.T_ARTIST)
                    .column(TArtist.C_NAME)
                    .where(TArtist.C_ID.in(subquery.subQuery()));

            query.setParameter("price", 100.0D);
            List<ArtistValueDTO> values = query.list(ArtistValueDTO.class);
            dumpCollection(values);

            assertTrue("Returned list of Subquery in Where has wrong size!", values.size() == 2);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testSubQueryAsColumn() throws Exception {
        // select a.*, (select count(*) from Painting p where p.artist_id =
        // a.id) from Artist
        try {
            Query subquery = db.query(TPainting.T_PAINTING).as("p")
                    .count()
                    .where(
                            TPainting.C_ARTIST.is(TArtist.C_ID.of("a"))
                    );

            Query query = db.query(TArtist.T_ARTIST).as("a")
                    .column(TArtist.C_NAME)
                    .column(subquery).as("value");
            List<ArtistValueDTO> values = query.list(ArtistValueDTO.class);
            dumpCollection(values);

            assertTrue("Returned list of Subquery in Column has wrong size!", values.size() == 3);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testNotExists() throws Exception {
        try {
            Query subquery = db.query(TPainting.T_PAINTING).as("p")
                    .column(TPainting.C_NAME)
                    .where(
                            TPainting.C_ARTIST.is(TArtist.C_ID.of("a"))
                    );

            Query query = db.query(TArtist.T_ARTIST).as("a")
                    .column(TArtist.C_NAME)
                    .where(exists(subquery).not());

            List<ArtistValueDTO> values = query.list(ArtistValueDTO.class);
            dumpCollection(values);

            assertTrue("No result for Not Exists", values != null && values.size() == 1);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testGroupBy() throws Exception {
        try {
            Query query = db.query(TArtist.T_ARTIST)
                    .column(TArtist.C_NAME)
                    .outer(TArtist.A_PAINTINGS).include(sum(TPainting.C_PRICE)).as("value").join()
                    .groupBy(1);
            List<ArtistValueDTO> values = query.list(ArtistValueDTO.class, false);
            dumpCollection(values);

            assertTrue("Wrong size for artist list.", values.size() == 3);
            ArtistValueDTO dto = values.get(0);
            assertTrue("Invalid GroupBy.", dto.getName() != null && dto.getValue() != null);
            dto = values.get(1);
            assertTrue("Invalid GroupBy.", dto.getName() != null && dto.getValue() != null);
            dto = values.get(2);
            assertTrue("Invalid GroupBy.", dto.getName() != null && dto.getValue() == null);

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testTransformer() throws Exception {
        try {
            Query query = db.query(TArtist.T_ARTIST).as("a")
                    .column(TArtist.C_NAME).as("name")
                    .outer(TArtist.A_PAINTINGS).include(sum(TPainting.C_PRICE)).as("value").join()
                    .groupBy(1);

            List<ArtistValueDTO> values = query.list(new SimpleAbstractDbRowTransformer<ArtistValueDTO>(db) {
                @Override
                public ArtistValueDTO transform(ResultSetWrapper rsw) throws SQLException {
                    ArtistValueDTO dto = new ArtistValueDTO();
                    dto.setName(toString(rsw, 1));
                    dto.setValue(toDecimal(rsw, 2));
                    return dto;
                }
            });
            dumpCollection(values);

            assertTrue("Wrong size for artist list.", values.size() == 3);
            ArtistValueDTO dto = values.get(0);
            assertTrue("Invalid GroupBy.", dto.getName() != null && dto.getValue() != null);
            dto = values.get(1);
            assertTrue("Invalid GroupBy.", dto.getName() != null && dto.getValue() != null);
            dto = values.get(2);
            assertTrue("Invalid GroupBy.", dto.getName() != null && dto.getValue() == null);

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testBeanTransformer() throws Exception {
        try {
            Query query = db.query(TArtist.T_ARTIST).as("a")
                    .column(TArtist.C_ID).as("id")
                    .column(TArtist.C_NAME).as("name")
                    .outer(TArtist.A_PAINTINGS).include(sum(TPainting.C_PRICE)).as("value").join()
                    .groupBy(1);

            List<ArtistValueDTO> values = query.list(ArtistValueDTO.class, false);
            dumpCollection(values);

            assertTrue("Wrong size for artist list.", values.size() == 3);
            ArtistValueDTO dto = values.get(0);
            assertTrue("Invalid GroupBy.",
                    dto.getId() != null &&
                            dto.getName() != null &&
                            dto.getValue() != null);
            dto = values.get(1);
            assertTrue("Invalid GroupBy.",
                    dto.getId() != null &&
                            dto.getName() != null &&
                            dto.getValue() != null);
            dto = values.get(2);
            assertTrue("Invalid GroupBy.",
                    dto.getId() != null &&
                            dto.getName() != null &&
                            dto.getValue() == null);

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testWithtChildren() {
        Query query = db.queryAll(TArtist.T_ARTIST).innerFetch(TArtist.A_PAINTINGS);

        String sql = db.getDriver().getSql(query);
        System.out.println("SQL: " + sql);

        List<Artist> artists = query.list(ArtistDAOBase.factory);
        dumpCollection(artists);

        assertTrue("Size of artist list is wrong!", artists.size() == 2);
        for (Artist artist : artists) {
            Set<Painting> paintings = artist.getPaintings();
            assertTrue("branch artist.paintings was not found", paintings != null);
            for (Painting painting : paintings) {
                assertTrue("branch artist.paintings.id was not found", painting.getId() != null);
            }
        }
    }

    @Test
    public void testWithtChildrenORMTransformer() throws Exception {
        try {
            Query query = db.queryAll(TArtist.T_ARTIST).innerFetch(TArtist.A_PAINTINGS);
            List<Artist> artists = query.list(Artist.class, true);
            // long version
            // List<Artist> artists = db.select(new
            // DomainBeanTransformer<Artist>(query, Artist.class, true));
            dumpCollection(artists);

            assertTrue("Size of artist list is wrong!", artists.size() == 2);

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testWithPartialChildrenORMTransformer() throws Exception {
        try {
            Query query = db.query(TArtist.T_ARTIST).all()
                    .inner(TArtist.A_PAINTINGS).include(TPainting.C_NAME)
                    .inner(TPainting.A_GALLERIES).on(TGallery.C_ID.is(1L)).fetch();
            List<Artist> artists = query.list(Artist.class, false);
            dumpCollection(artists);

            assertTrue("Size of artist list is wrong!", artists.size() == 3);
            for (Artist artist : artists) {
                Set<Painting> paintings = artist.getPaintings();
                assertTrue("branch artist.paintings was not found", paintings != null);
                for (Painting painting : paintings) {
                    assertTrue("branch artist.paintings.id was found", painting.getId() == null);
                    assertTrue("branch artist.paintings.name was not found", painting.getName() != null);
                    assertTrue("branch artist.paintings.galleries was found", painting.getGalleries().size() == 1);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testWithChildrenOnPaintingName() {
        Query query = db.queryAll(TArtist.T_ARTIST)
                .outer(TArtist.A_PAINTINGS)
                .on(TPainting.C_NAME.like("Blue%"))
                .fetch()
                .order(TArtist.C_ID).asc()
                .orderBy(TPainting.C_NAME).asc();

        List<Artist> artists = query.list(new ArtistDAOBase());
        dumpCollection(artists);

        assertTrue("Size of artist list is wrong!", artists.size() == 3);
        Set<Painting> paintings = artists.get(0).getPaintings();
        assertTrue("branch artist.paintings has wrong size. Must be 1.", paintings != null && paintings.size() == 1);
    }

    @Test
    public void testWithNoChildrenOnPaintingName() {
        Artist artist = db.queryAll(TArtist.T_ARTIST)
                .all()
                .outer(TArtist.A_PAINTINGS)
                .on(TPainting.C_NAME.is("XPTO"))
                .fetch()
                .where(TArtist.C_ID.is(1L))
                .unique(Artist.class);

        dump(artist);

        assertTrue("branch artist.paintings is not null.", artist.getPaintings() == null);
    }

    @Test
    public void testOuterFetchManyToMany1() throws Exception {
        try {
            List<Gallery> galleries = db.queryAll(TGallery.T_GALLERY)
                    .outerFetch(TGallery.A_PAINTINGS)
                    .order(TGallery.C_ID).asc()
                    .list(Gallery.class, true);
            dumpCollection(galleries);

            assertTrue("Size of galleries list is wrong!", galleries.size() == 2);
            Set<Painting> paintings = galleries.get(0).getPaintings();
            assertTrue("branch galleries.paintings has wrong size. Must be 3.", paintings != null && paintings.size() == 3);
            paintings = galleries.get(1).getPaintings();
            assertTrue("branch galleries.paintings has wrong size. Must be 2.", paintings != null && paintings.size() == 2);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testOuterFetchManyToMany2() throws Exception {
        try {
            Query query = db.queryAll(TPainting.T_PAINTING)
                    .outerFetch(TPainting.A_GALLERIES)
                    .order(TArtist.C_ID).asc();

            List<Painting> paintings = query.list(Painting.class, true);
            dumpCollection(paintings);

            assertTrue("Size of paintings list is wrong!", paintings.size() == 4);
            Set<Gallery> galleries = paintings.get(0).getGalleries();
            assertTrue("branch paintings[0].galleries has wrong size. Must be 2.", galleries != null && galleries.size() == 2);
            galleries = paintings.get(1).getGalleries();
            assertTrue("branch paintings[1].galleries has wrong size. Must be 2.", galleries != null && galleries.size() == 2);
            galleries = paintings.get(2).getGalleries();
            assertTrue("branch paintings[2].galleries has wrong size. Must be 1.", galleries != null && galleries.size() == 1);
            galleries = paintings.get(3).getGalleries();
            assertTrue("branch paintings[3].galleries has wrong size. Must be null.", galleries == null);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testOuterFetchWithAll() throws Exception {
        try {
            Query query = db.queryAll(TArtist.T_ARTIST)
                    .outerFetch(TArtist.A_PAINTINGS, TPainting.A_GALLERIES);
            List<Artist> artists = query.list(Artist.class);
            dumpCollection(artists);

            assertTrue("Wrong size for artist List", artists.size() == 3);
            assertTrue("artist[0].paintings is null.", artists.get(0).getPaintings() != null);
            for (Painting paint : artists.get(0).getPaintings()) {
                assertTrue("Gallery is null", paint.getGalleries() != null);
            }
            assertTrue("artist[1].paintings is null.", artists.get(1).getPaintings() != null);
            Iterator<Painting> it = artists.get(1).getPaintings().iterator();
            Painting paint = it.next();
            assertTrue("Gallery is null", paint.getGalleries() != null);
            paint = it.next();
            assertTrue("Gallery is not null", paint.getGalleries() == null);
            assertTrue("artist[2].paintings is not null.", artists.get(2).getPaintings() == null);

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testInnerJoin() throws Exception {
        // list all Paintings from Pablo Picasso (id = 1)
        try {
            Query query = db.queryAll(TPainting.T_PAINTING)
                    .inner(TPainting.A_ARTIST)
                    .on(TArtist.C_ID.is(1L))
                    .join();
            List<Painting> values = query.list(Painting.class);
            dumpCollection(values);

            assertTrue("Wrong size for painting list", values.size() == 2);
            for (Painting paint : values) {
                assertTrue("Artist is not null", paint.getArtist() == null);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testOuterJoin() throws Exception {
        // Ex: list all Artists and the price of each painting, even if the
        // Artist doesn’t have paintings.
        try {
            Query query = db.query(TArtist.T_ARTIST)
                    .column(TArtist.C_NAME)
                    .outer(TArtist.A_PAINTINGS)
                    .include(TPainting.C_PRICE).as("value")
                    .join(); // this will add the includes at the TOP of the tree
            List<ArtistValueDTO> values = query.list(ArtistValueDTO.class, false);
            dumpCollection(values);

            assertTrue("Wrong size for outer join.", values.size() == 5);

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testIncludeJoinAndFetch() throws Exception {
        // Ex: list all Artists and the price of each painting, even if the
        // Artist doesn’t have paintings.
        try {
            List<ArtistValueDTO> values = db.query(TArtist.T_ARTIST)
                    .column(TArtist.C_NAME)
                    .inner(TArtist.A_PAINTINGS)
                    .include(TPainting.C_PRICE).as("value")
                    .join() // this will add the includes at the TOP of the tree
                    .inner(TArtist.A_PAINTINGS)
                    .include(TPainting.C_NAME)
                    .fetch() // but this will dump the values in corresponding object tree branch
                    .list(ArtistValueDTO.class, false);
            dumpCollection(values);

            assertTrue("Wrong size for outer join. Expected 4, got " + values.size(), values.size() == 4);
            for (ArtistValueDTO val : values) {
                assertTrue("Invalid Name", val.getName() != null);
                assertTrue("Invalid Value", val.getValue() != null);
                assertTrue("Invalid Value.paintings", val.getPaintings() != null);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testJoinAndFetch() {
        /*
         * This example shows the difference between join() and fetch().
         * join() enforces only the constraint where fetch also brings all data
         * from the tables in its path
         */
        List<Painting> paintings = db.query(TPainting.T_PAINTING).all()
                .inner(TPainting.A_ARTIST).on(TArtist.C_NAME.like("Pablo%")).join()
                .inner(TPainting.A_GALLERIES).fetch()
                .list(Painting.class);

        dumpCollection(paintings);

        assertTrue("Wrong size for paintings.", paintings.size() == 2);
        for (Painting paint : paintings) {
            assertTrue("Artis should be null.", paint.getArtist() == null);
        }
    }

    @Test
    public void testSimpleFetch() {
        List<Artist> artists = db.query(TArtist.T_ARTIST).all()
                .order(TArtist.C_NAME)
                .outer(TArtist.A_PAINTINGS).fetch()
                .list(new MapBeanTransformer<>(Artist.class));

        dumpCollection(artists);

        assertTrue("Wrong size for artists. Expected 3, got " + artists.size(), artists.size() == 3);
        Set<Painting> paintings = artists.get(0).getPaintings();
        assertTrue("Wrong size for Paintings, for artist[0]. Expected null, got not null", paintings == null);
        int size = artists.get(1).getPaintings().size();
        assertTrue("Wrong size for Paintings, for artist[1]. Expected 2, got " + size, size == 2);
        size = artists.get(2).getPaintings().size();
        assertTrue("Wrong size for Paintings, for artist[2]. Expected 3, got " + size, size == 2);
    }

    @Test
    public void testCustomFunction() {
        // using custom functions
        Artist artist = db.query(TArtist.T_ARTIST)
                .column(FunctionExt.ifNull(TArtist.C_BIRTHDAY, new Date())).as("birthday")
                .where(TArtist.C_ID.is(1L))
                .unique(Artist.class);

        assertTrue("Birthday is null when using custom function ifNull.", artist.getBirthday() != null);
    }

    @Test
    public void testTemporal() {
        // INSERT
        Insert insert = db.insert(TTemporal.T_TEMPORAL).retriveKeys(false)
                .sets(TTemporal.C_ID, TTemporal.C_CLOCK, TTemporal.C_TODAY, TTemporal.C_NOW, TTemporal.C_INSTANT);

        MyTime myTime = new MyTime();
        MyDate myDate = new MyDate();
        MyDateTime myDateTime = new MyDateTime();
        Date date = new Date();
        insert.values(1L, myTime, myDate, myDateTime, date).execute();

        Temporal temporal = db.query(TTemporal.T_TEMPORAL).all()
                .where(TTemporal.C_ID.is(1L))
                .unique(Temporal.class);
        dump(temporal);

        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.sssss");
        System.out.println("Time ===> " + sdf.format(myTime) + " <=> " + sdf.format(temporal.getClock()));
        String expected = sdf.format(myTime);
        String actual = sdf.format(temporal.getClock());
        assertTrue("Clock is incorrect! Expected " + expected + ", got " + actual, myTime.equals(temporal.getClock()));

        sdf = new SimpleDateFormat("yyyy-MM-dd");
        assertTrue("Today is incorrect!", sdf.format(myDate).equals(sdf.format(temporal.getToday())));

        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        assertTrue("Now is incorrect!", sdf.format(myDateTime).equals(sdf.format(temporal.getNow())));

        //sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        System.out.println("Timestamp ===> " + sdf.format(date) + " <=> " + sdf.format(temporal.getInstant()));
        assertTrue("Instant is incorrect!", sdf.format(date).equals(sdf.format(temporal.getInstant())));
    }

    @Test
    public void testCRUD() {
        // INSERT
        Stopwatch sw = Stopwatch.createAndStart();
        db.insert(TArtist.T_ARTIST)
                .set(TArtist.C_ID, 4L)
                .set(TArtist.C_VERSION, 1)
                .set(TArtist.C_GENDER, EGender.MALE)
                .set(TArtist.C_NAME, "matisse")
                .execute();
        sw.showTotal("INSERT");
        sw.reset();

        // UPDATE
        sw.start();
        Update update = new Update(db, TArtist.T_ARTIST)
                .set(TArtist.C_NAME, "Henri Matisse")
                .set(TArtist.C_VERSION, 2)
                // .set(TArtist.C_VERSION, 2L)
                .where(TArtist.C_ID.is(4L), TArtist.C_VERSION.is(param("ver")));
        update.setParameter("ver", 1L);
        update.execute();
        sw.stop().showTotal("UPDATE").reset();

        // DELETE
        sw.start();
        Delete delete = new Delete(db, TArtist.T_ARTIST)
                .where(TArtist.C_ID.is(param("id")));
        delete.setParameter("id", 4L);
        delete.execute();
        sw.stop().showTotal("DELETE");
    }

    @Test
    public void testInsert() {
        // INSERT
        Insert insert = db.insert(TArtist.T_ARTIST).retriveKeys(false)
                .sets(TArtist.C_ID, TArtist.C_VERSION, TArtist.C_GENDER, TArtist.C_NAME, TArtist.C_BIRTHDAY);

        Stopwatch sw = Stopwatch.createAndStart();
        insert.values(4L, 1L, EGender.MALE, "matisse", new MyDateTime(System.currentTimeMillis() - 90 * YEAR)).execute();
        sw.stop().showTotal("INSERT execute").reset();

        sw.start();
        insert.values(5L, 1L, EGender.FEMALE, "Jane DOE", null).execute();
        sw.stop().showTotal("INSERT execute").reset();

        sw.start();
        insert.values(6L, 1, EGender.FEMALE, "Jane DOE Sister", new MyDateTime()).execute();
        sw.stop().showTotal("INSERT execute").reset();
    }

    @Test
    public void testQuickDelete() {
        Painting painting = new Painting();
        painting.setId(4L);
        try {
            painting.setVersion(3); // wrong version
            db.delete(TPainting.T_PAINTING).submit(painting);
            assertTrue("Version was not validated!", false);
        } catch (OptimisticLockException e) {
            assertTrue(true);
        }
        painting.setVersion(1); // wrong version
        Stopwatch sw = Stopwatch.createAndStart();
        boolean deleted = db.delete(TPainting.T_PAINTING).execute(painting);
        sw.stop().showTotal("SUBMIT DELETE").reset();
        assertTrue("Painting was not FAST deleted!", deleted);
    }

    @Test
    public void testUpdateCache() {
        Artist artist = db.query(TArtist.T_ARTIST).all()
                .where(TArtist.C_ID.is(1L)).unique(Artist.class);

        artist.setName("Jane Mnomonic");
        db.update(TArtist.T_ARTIST).submit(artist);
        assertTrue("Incorrect version!", artist.getVersion().equals(2));

        artist.setGender(EGender.FEMALE);
        db.update(TArtist.T_ARTIST).submit(artist);
        assertTrue("Incorrect version!", artist.getVersion().equals(3));
    }

    @Test
    public void testQuickCRUD() throws Exception {
        try {
            // INSERT
            Artist artist = new Artist();
            artist.setName("John Mnomonic");
            artist.setGender(EGender.MALE);
            db.insert(TArtist.T_ARTIST).submit(artist);

            dump(artist);
            assertTrue("No id returned when inserting!", artist.getId() != null);
            assertTrue("Generic Class for Id should be Long.class", artist.getId().getClass() == Long.class);

            List<Artist> artists = db.query(TArtist.T_ARTIST).all()
                    .list(Artist.class);
            dumpCollection(artists);

            // UPDATE
            artist.setName("Jane Mnomonic");
            artist.setGender(EGender.FEMALE);
            db.update(TArtist.T_ARTIST).submit(artist);

            dump(artist);
            assertTrue("Incorrect version!", artist.getVersion().equals(2));

            artists = db.query(TArtist.T_ARTIST).all()
                    .list(Artist.class);
            dumpCollection(artists);

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void testBeanCRUD() {
        // INSERT
        Insert insert = db.insert(TArtist.T_ARTIST);

        Artist artist = new Artist();
        artist.setName("John Mnomonic");
        artist.setGender(EGender.MALE);
        artist.setVersion(1);
        insert.set(artist).execute();

        artist = new Artist();
        artist.setName("Jane Mnomonic");
        artist.setGender(EGender.FEMALE);
        artist.setVersion(1);
        Map<Column<?>, Object> keys = insert.set(artist).execute();

        Object key = keys.get(TArtist.C_ID);
        artist.setId(((Number) key).longValue());
        artist.setName("Jane Doe");
        db.update(TArtist.T_ARTIST).set(artist).execute();
    }

    @Test
    public void testInsertImage() throws IOException {
        BinStore bc = new BinStore();
        // throws IOException if file is not found
        bc.set(new File("./src/test/resources/StarryNight.jpg"));

        Insert insert = new Insert(db, TImage.T_IMAGE)
                .set(TImage.C_VERSION, 1)
                .set(TImage.C_CONTENT, bc);

        Map<Column<?>, Object> keys = insert.execute();
        for (Entry<Column<?>, Object> entry : keys.entrySet()) {
            assertTrue(entry.getKey().getName() + " was null", entry.getValue() != null);
        }
    }

    @Test
    public void testLoadImage() throws IOException {
        List<BinStore> images = db.query(TImage.T_IMAGE)
                .column(TImage.C_CONTENT)
                .listRaw(BinStore.class);
        for (BinStore image : images) {
            System.out.println("size: " + image.get().length);
        }
        assertTrue("Wrong size for Image List!", images.size() == 4);
    }

    @Test
    public void testOnePaintingToOneImage() {
        List<Painting> paintings = db.query(TPainting.T_PAINTING).all()
                .outerFetch(TPainting.A_IMAGE)
                .list(Painting.class);

        dumpCollection(paintings);

        assertTrue("Size of paintings list is wrong!", paintings.size() == 4);
        Image image = paintings.get(0).getImage();
        assertTrue("branch paintings[0].image was not returned.", image != null);
    }

    @Test
    public void testOnePaintingToOneArtist() {
        List<Painting> paintings = db.query(TPainting.T_PAINTING).all()
                .outerFetch(TPainting.A_ARTIST)
                .list(Painting.class);

        dumpCollection(paintings);

        assertTrue("Size of paintings list is wrong!", paintings.size() == 4);
        Artist artist = paintings.get(0).getArtist();
        assertTrue("branch paintings[0].artist was not returned.", artist != null);
    }

    @Test
    public void testLoadImageBytes() {
        List<ImageDTO> images = db.query(TImage.T_IMAGE).all()
                .list(ImageDTO.class);
        dumpCollection(images);
        for (ImageDTO image : images) {
            byte[] content = image.getContent();
            assertTrue("Image data is null!", content != null && content.length > 0);
        }
    }

    @Test
    public void testInsertImageBytes() throws IOException {
        BinStore bc = new BinStore();
        // throws IOException if file is not found
        bc.set(new File("./src/test/resources/StarryNight.jpg"));

        ImageDTO image = new ImageDTO();
        image.setVersion(1);
        image.setContent(bc.get());
        Map<Column<?>, Object> keys = db.insert(TImage.T_IMAGE).set(image).execute();
        dump(image);

        Long key = (Long) keys.get(TImage.C_ID);
        image = db.query(TImage.T_IMAGE).all()
                .where(TImage.C_ID.is(key)).unique(ImageDTO.class);
        byte[] content = image.getContent();
        assertTrue("Image data is null!", content != null && content.length > 0);
    }

    @Test
    public void testNumericEnum() throws IOException {
        Insert insert = db.insert(TEmployee.T_EMPLOYEE)
                .sets(TEmployee.C_ID, TEmployee.C_NAME, TEmployee.C_SEX, TEmployee.C_PAY_GRADE, TEmployee.C_CREATION);
        insert.values(1, "Oscar", true, EPayGrade.LOW, new Date()).execute();
        insert.values(2, "Maria", false, EPayGrade.HIGH, new Date()).execute();

        List<Employee> employees = db.query(TEmployee.T_EMPLOYEE).all()
                .order(TEmployee.C_ID)
                .list(Employee.class);

        dumpCollection(employees);

        assertTrue("Wrong list size for employees!", employees.size() == 2);
        assertTrue("Wrong pay grade type!", employees.get(0).getPayGrade() == EPayGrade.LOW);
        assertTrue("Wrong pay grade type!", employees.get(1).getPayGrade() == EPayGrade.HIGH);
    }

    @Test
    public void testSimpleCase() {
        Long sale = db.query(TPainting.T_PAINTING)
                .column(
                        sum(
                                with(TPainting.C_NAME)
                                        .when("Blue Nude").then(10)
                                        .otherwise(asIs(20)) // asIs(): value is written as is to the query
                                        .end()
                        )
                )
                .uniqueLong();

        assertTrue("Wrong sale value for Paintings! Expected 70, got " + sale, sale.equals(70L));
    }

    public static class Classification {
        private String name;
        private String category;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getCategory() {
            return category;
        }

        public void setCategory(String category) {
            this.category = category;
        }
    }

    @Test
    public void testSearchedCase() {
        List<Classification> c = db.query(TPainting.T_PAINTING)
                .column(TPainting.C_NAME) // default maps to field name
                .column(
                        when(TPainting.C_PRICE.gt(150D)).then("expensive")
                                .when(TPainting.C_PRICE.range(50D, 150D)).then("normal")
                                .otherwise("cheap")
                                .end()
                )
                .as("category") // maps to field category
                .order(TPainting.C_PRICE).desc()
                .list(Classification.class);

        assertTrue("Wrong category value for Paintings!", "expensive".equals(c.get(0).getCategory()));
        assertTrue("Wrong category value for Paintings!", "normal".equals(c.get(1).getCategory()));
        assertTrue("Wrong category value for Paintings!", "normal".equals(c.get(2).getCategory()));
        assertTrue("Wrong category value for Paintings!", "cheap".equals(c.get(3).getCategory()));

    }
}
