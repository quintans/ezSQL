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
import com.github.quintans.ezSQL.orm.app.daos.ArtistDAOMapper;
import com.github.quintans.ezSQL.orm.app.daos.PaintingDAOMapper;
import com.github.quintans.ezSQL.orm.app.domain.Artist;
import com.github.quintans.ezSQL.orm.app.domain.EGender;
import com.github.quintans.ezSQL.orm.app.domain.EPayGrade;
import com.github.quintans.ezSQL.orm.app.domain.Employee;
import com.github.quintans.ezSQL.orm.app.domain.Gallery;
import com.github.quintans.ezSQL.orm.app.domain.Image;
import com.github.quintans.ezSQL.orm.app.domain.Painting;
import com.github.quintans.ezSQL.orm.app.domain.Temporal;
import com.github.quintans.ezSQL.orm.app.dtos.ArtistValueDTO;
import com.github.quintans.ezSQL.orm.app.dtos.ImageDTO;
import com.github.quintans.ezSQL.orm.app.mappings.TArtist;
import com.github.quintans.ezSQL.orm.app.mappings.TEmployee;
import com.github.quintans.ezSQL.orm.app.mappings.TGallery;
import com.github.quintans.ezSQL.orm.app.mappings.TImage;
import com.github.quintans.ezSQL.orm.app.mappings.TPainting;
import com.github.quintans.ezSQL.orm.app.mappings.TTemporal;
import com.github.quintans.ezSQL.orm.extended.FunctionExt;
import com.github.quintans.ezSQL.toolkit.io.BinStore;
import com.github.quintans.ezSQL.transformers.MapTransformer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.github.quintans.ezSQL.dml.Definition.asIs;
import static com.github.quintans.ezSQL.dml.Definition.exists;
import static com.github.quintans.ezSQL.dml.Definition.param;
import static com.github.quintans.ezSQL.dml.Definition.sum;
import static com.github.quintans.ezSQL.dml.Definition.when;
import static com.github.quintans.ezSQL.dml.Definition.with;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class TestStandard extends TestBootstrap {
  private static final long YEAR = 365L * 24L * 3600000L;

  public TestStandard(String environment) {
    super(environment);
  }

  @Test
  public void testEmptyTable() {
    tm.transactionNoResult(db -> {
      List<Employee> list = db.query(TEmployee.T_EMPLOYEE).all().list(Employee.class);
      assertTrue("List should be empty.", list.isEmpty());
    });
  }

  @Test
  public void testLoadAssociation() {
    tm.transactionNoResult(db -> {
      Artist artist = db.query(TArtist.T_ARTIST).all()
          .where(TArtist.C_ID.is(1L))
          .select(Artist.class);
      Set<Painting> paintings = db.loadAssociation(artist, TArtist.A_PAINTINGS);
      dumpCollection(paintings);
      dump(artist);

      assertTrue("Wrong set size when loading association paintings of Artist.", paintings != null && paintings.size() == 2);
    });
  }

  @Test
  public void testEnum() {
    tm.transactionNoResult(db -> {
      Query query = db.query(TArtist.T_ARTIST).column(TArtist.C_GENDER);
      List<EGender> genders = query.listRaw(EGender.class);
      dumpCollection(genders);

      genders = query.listRaw(EGender.class);
      dumpCollection(genders);

      assertEquals("Wrong size for gender list.", 3, genders.size());
      assertTrue("Gender are incorrect null.",
          genders.get(0) == EGender.MALE &&
              genders.get(1) == EGender.MALE &&
              genders.get(2) == EGender.FEMALE
      );
    });
  }

  @Test
  public void testAnd() {
    tm.transactionNoResult(db -> {
      List<Artist> artists = db.queryAll(TArtist.T_ARTIST)
          .where(
              TArtist.C_NAME.like("%n%").and(TArtist.C_GENDER.is(EGender.FEMALE))
          )
          .list(Artist.class);
      dumpCollection(artists);

      assertEquals("Wrong list size when testing Artist AND! expected 2!" + artists.size(), 1, artists.size());
    });
  }

  @Test
  public void testOr() {
    tm.transactionNoResult(db -> {
      List<Artist> artists = db.queryAll(TArtist.T_ARTIST)
          .where(
              TArtist.C_NAME.like("J%").or(TArtist.C_GENDER.is(EGender.MALE))
          )
          .list(Artist.class);
      dumpCollection(artists);

      assertEquals("Wrong list size when testing Artist AND! expected 3!" + artists.size(), 3, artists.size());
    });
  }

  @Test
  public void testCyclicFkReference() {
    tm.transactionNoResult(db -> {
      Query query = db.queryAll(TArtist.T_ARTIST).innerFetch(TArtist.A_PAINTINGS);
      List<Artist> artists = query.list(new MapTransformer<>(query, true, Artist.class, new ArtistDAOMapper()));
      dumpCollection(artists);

      assertEquals("Wrong size for artist list.", 2, artists.size());
    });
  }

  @Test
  public void testCyclicFkReference2() {
    tm.transactionNoResult(db -> {
      Query query = db.queryAll(TPainting.T_PAINTING).innerFetch(TPainting.A_ARTIST);
      List<Painting> entities = query.list(new MapTransformer<>(query, true, Painting.class, new PaintingDAOMapper()));
      dumpCollection(entities);

      assertEquals("Wrong size for artist list.", 4, entities.size());
    });
  }

  @Test
  public void testListRaw() {
    tm.transactionNoResult(db -> {
      Stopwatch sw = Stopwatch.createAndStart();

      Query query = db.query(TArtist.T_ARTIST)
          .column(TArtist.C_ID)
          .column(TArtist.C_NAME);
      List<Object[]> list = query.listRaw(Long.class, String.class);

      sw.showTotal(null);

      dumpRaw(list);

      assertTrue(list.size() > 0);
    });
  }

  @Test
  public void testSimpleTransformer() {
    tm.transactionNoResult(db -> {
      Query query = db.query(TArtist.T_ARTIST)
          .column(TArtist.C_ID)
          .column(TArtist.C_NAME)
          .column(TArtist.C_GENDER);

      List<Artist> values = query.list(r -> {
        Artist dto = new Artist();
        dto.setId(r.get(1, Long.class));
        dto.setName(r.get(2, String.class));
        dto.setGender(r.get(3, EGender.class));
        return dto;
      });
      dumpCollection(values);

      assertEquals("Wrong size for artist list.", 3, values.size());
    });
  }

  @Test
  public void testSimpleBeanTransformer() {
    tm.transactionNoResult(db -> {
      Query query = db.queryAll(TArtist.T_ARTIST).orderBy(TArtist.C_NAME.desc());
      // List<Artist> values = db.select(new
      // BeanTransformer<Artist>(query, Artist.class)); // long version
      // query.setFirstResult(10);
      query.skip(1).limit(1);
      List<Artist> values = query.list(Artist.class);
      dumpCollection(values);

      assertEquals("List returned form list is incorrect!", 1, values.size());
    });
  }

  @Test
  public void testSubQueryInWhere() {
    tm.transactionNoResult(db -> {
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

      assertEquals("Returned list of Subquery in Where has wrong size!", 2, values.size());
    });
  }

  @Test
  public void testSubQueryAsColumn() {
    tm.transactionNoResult(db -> {
      // select a.*, (select count(*) from Painting p where p.artist_id =
      // a.id) from Artist
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

      assertEquals("Returned list of Subquery in Column has wrong size!", 3, values.size());
    });
  }

  @Test
  public void testNotExists() {
    tm.transactionNoResult(db -> {
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
    });
  }

  @Test
  public void testGroupBy() {
    tm.transactionNoResult(db -> {
      Query query = db.query(TArtist.T_ARTIST)
          .column(TArtist.C_NAME)
          .orderOn(TArtist.C_NAME.asc())
          .outer(TArtist.A_PAINTINGS).include(sum(TPainting.C_PRICE)).as("value").join()
          .groupBy(1);
      List<ArtistValueDTO> values = query.list(ArtistValueDTO.class, false);
      dumpCollection(values);

      assertEquals("Wrong size for artist list.", 3, values.size());
      ArtistValueDTO dto = values.get(0);
      assertTrue("Invalid GroupBy.", dto.getName() != null && dto.getValue() == null);
      dto = values.get(1);
      assertTrue("Invalid GroupBy.", dto.getName() != null && dto.getValue() != null);
      dto = values.get(2);
      assertTrue("Invalid GroupBy.", dto.getName() != null && dto.getValue() != null);
    });
  }

  @Test
  public void testTransformer() {
    tm.transactionNoResult(db -> {
      Query query = db.query(TArtist.T_ARTIST).as("a")
          .column(TArtist.C_NAME).as("name")
          .orderOn(TArtist.C_NAME.asc())
          .outer(TArtist.A_PAINTINGS).include(sum(TPainting.C_PRICE)).as("value").join()
          .groupBy(1);

      List<ArtistValueDTO> values = query.list(r -> {
        ArtistValueDTO dto = new ArtistValueDTO();
        dto.setName(r.get(1, String.class));
        dto.setValue(r.get(2, Double.class));
        return dto;
      });
      dumpCollection(values);

      assertEquals("Wrong size for artist list.", 3, values.size());
      ArtistValueDTO dto = values.get(0);
      assertTrue("Invalid GroupBy.", dto.getName() != null && dto.getValue() == null);
      dto = values.get(1);
      assertTrue("Invalid GroupBy.", dto.getName() != null && dto.getValue() != null);
      dto = values.get(2);
      assertTrue("Invalid GroupBy.", dto.getName() != null && dto.getValue() != null);

    });
  }

  @Test
  public void testBeanTransformer() {
    tm.transactionNoResult(db -> {
      Query query = db.query(TArtist.T_ARTIST).as("a")
          .column(TArtist.C_ID).as("id")
          .column(TArtist.C_NAME).as("name")
          .outer(TArtist.A_PAINTINGS).include(sum(TPainting.C_PRICE)).as("value").join()
          .orderBy(TArtist.C_ID.asc())
          .groupBy(1);

      List<ArtistValueDTO> values = query.list(ArtistValueDTO.class, false);
      dumpCollection(values);

      assertEquals("Wrong size for artist list.", 3, values.size());
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

    });
  }

  @Test
  public void testWithtChildren() {
    tm.transactionNoResult(db -> {
      Query query = db.queryAll(TArtist.T_ARTIST).innerFetch(TArtist.A_PAINTINGS);

      String sql = db.getTranslator().getSql(query);
      System.out.println("SQL: " + sql);

      List<Artist> artists = query.list(new MapTransformer<>(query, true, Artist.class, new ArtistDAOMapper()));
      dumpCollection(artists);

      assertEquals("Size of artist list is wrong!", 2, artists.size());
      for (Artist artist : artists) {
        Set<Painting> paintings = artist.getPaintings();
        assertNotNull("branch artist.paintings was not found", paintings);
        for (Painting painting : paintings) {
          assertNotNull("branch artist.paintings.id was not found", painting.getId());
        }
      }
    });
  }

  @Test
  public void testWithtChildrenORMTransformer() {
    tm.transactionNoResult(db -> {
      Query query = db.queryAll(TArtist.T_ARTIST).innerFetch(TArtist.A_PAINTINGS);
      List<Artist> artists = query.list(Artist.class, true);
      // long version
      // List<Artist> artists = db.select(new
      // DomainBeanTransformer<Artist>(query, Artist.class, true));
      dumpCollection(artists);

      assertEquals("Size of artist list is wrong!", 2, artists.size());
    });
  }

  @Test
  public void testWithPartialChildrenORMTransformer() {
    tm.transactionNoResult(db -> {
      Query query = db.query(TArtist.T_ARTIST).all()
          .inner(TArtist.A_PAINTINGS).include(TPainting.C_NAME)
          .inner(TPainting.A_GALLERIES).on(TGallery.C_ID.is(1L)).fetch();
      List<Artist> artists = query.list(Artist.class, false);
      dumpCollection(artists);

      assertEquals("Size of artist list is wrong!", 3, artists.size());
      for (Artist artist : artists) {
        Set<Painting> paintings = artist.getPaintings();
        assertNotNull("branch artist.paintings was not found", paintings);
        for (Painting painting : paintings) {
          assertNull("branch artist.paintings.id was found", painting.getId());
          assertNotNull("branch artist.paintings.name was not found", painting.getName());
          assertEquals("branch artist.paintings.galleries was found", 1, painting.getGalleries().size());
        }
      }
    });
  }

  @Test
  public void testWithChildrenOnPaintingName() {
    tm.transactionNoResult(db -> {
      Query query = db.queryAll(TArtist.T_ARTIST)
          .outer(TArtist.A_PAINTINGS)
          .on(TPainting.C_NAME.like("Blue%"))
          .fetch()
          .orderBy(TArtist.C_ID.asc())
          .orderOn(TPainting.C_NAME.asc());

      List<Artist> artists = query.list(new MapTransformer<>(query, true, Artist.class, new ArtistDAOMapper()));
      dumpCollection(artists);

      assertEquals("Size of artist list is wrong!", 3, artists.size());
      Set<Painting> paintings = artists.get(0).getPaintings();
      assertTrue("branch artist.paintings has wrong size. Must be 1.", paintings != null && paintings.size() == 1);
    });
  }

  @Test
  public void testWithNoChildrenOnPaintingName() {
    tm.transactionNoResult(db -> {
      Artist artist = db.queryAll(TArtist.T_ARTIST)
          .all()
          .outer(TArtist.A_PAINTINGS)
          .on(TPainting.C_NAME.is("XPTO"))
          .fetch()
          .where(TArtist.C_ID.is(1L))
          .unique(Artist.class);

      dump(artist);

      assertNull("branch artist.paintings is not null.", artist.getPaintings());
    });
  }

  @Test
  public void testOuterFetchManyToMany1() {
    tm.transactionNoResult(db -> {
      List<Gallery> galleries = db.queryAll(TGallery.T_GALLERY)
          .outerFetch(TGallery.A_PAINTINGS)
          .orderBy(TGallery.C_ID.asc())
          .list(Gallery.class, true);
      dumpCollection(galleries);

      assertEquals("Size of galleries list is wrong!", 2, galleries.size());
      Set<Painting> paintings = galleries.get(0).getPaintings();
      assertTrue("branch galleries.paintings has wrong size. Must be 3.", paintings != null && paintings.size() == 3);
      paintings = galleries.get(1).getPaintings();
      assertTrue("branch galleries.paintings has wrong size. Must be 2.", paintings != null && paintings.size() == 2);
    });
  }

  @Test
  public void testOuterFetchManyToMany2() {
    tm.transactionNoResult(db -> {
      Query query = db.queryAll(TPainting.T_PAINTING)
          .outerFetch(TPainting.A_GALLERIES)
          .orderBy(TPainting.C_ID.asc());

      List<Painting> paintings = query.list(Painting.class, true);
      dumpCollection(paintings);

      assertEquals("Size of paintings list is wrong!", 4, paintings.size());
      Set<Gallery> galleries = paintings.get(0).getGalleries();
      assertTrue("branch paintings[0].galleries has wrong size. Must be 2.", galleries != null && galleries.size() == 2);
      galleries = paintings.get(1).getGalleries();
      assertTrue("branch paintings[1].galleries has wrong size. Must be 2.", galleries != null && galleries.size() == 2);
      galleries = paintings.get(2).getGalleries();
      assertTrue("branch paintings[2].galleries has wrong size. Must be 1.", galleries != null && galleries.size() == 1);
      galleries = paintings.get(3).getGalleries();
      assertNull("branch paintings[3].galleries has wrong size. Must be null.", galleries);
    });
  }

  @Test
  public void testOuterFetchWithAll() {
    tm.transactionNoResult(db -> {
      Query query = db.queryAll(TArtist.T_ARTIST)
          .outer(TArtist.A_PAINTINGS)
          .outer(TPainting.A_GALLERIES)
          .fetch()
          .orderBy(TArtist.C_ID.asc())
          .orderBy(TPainting.C_ID.asc())
          .orderBy(TGallery.C_ID.asc());
      List<Artist> artists = query.list(Artist.class);
      dumpCollection(artists);

      assertEquals("Wrong size for artist List", 3, artists.size());
      assertEquals("artist[0].paintings.size", 2, artists.get(0).getPaintings().size());
      assertEquals("artist[1].paintings.size", 2, artists.get(1).getPaintings().size());
      assertNull("artist[2].paintings is not null", artists.get(2).getPaintings());
      for (Painting paint : artists.get(0).getPaintings()) {
        assertNotNull("Gallery is null", paint.getGalleries());
      }
      Iterator<Painting> it = artists.get(1).getPaintings().iterator();
      Painting paint = it.next();
      assertNotNull("Gallery is null", paint.getGalleries());

      paint = it.next();
      assertNull("Gallery is not null", paint.getGalleries());
      assertNull("artist[2].paintings is not null.", artists.get(2).getPaintings());
    });
  }

  @Test
  public void testInnerJoin() {
    // list all Paintings from Pablo Picasso (id = 1)
    tm.transactionNoResult(db -> {
      Query query = db.queryAll(TPainting.T_PAINTING)
          .inner(TPainting.A_ARTIST)
          .on(TArtist.C_ID.is(1L))
          .join();
      List<Painting> values = query.list(Painting.class);
      dumpCollection(values);

      assertEquals("Wrong size for painting list", 2, values.size());
      for (Painting paint : values) {
        assertNull("Artist is not null", paint.getArtist());
      }
    });
  }

  @Test
  public void testOuterJoin() {
    // Ex: list all Artists and the price of each painting, even if the
    // Artist doesn’t have paintings.
    tm.transactionNoResult(db -> {
      Query query = db.query(TArtist.T_ARTIST)
          .column(TArtist.C_NAME)
          .outer(TArtist.A_PAINTINGS)
          .include(TPainting.C_PRICE).as("value")
          .join(); // this will add the includes at the TOP of the tree
      List<ArtistValueDTO> values = query.list(ArtistValueDTO.class, false);
      dumpCollection(values);

      assertEquals("Wrong size for outer join.", 5, values.size());

    });
  }

  @Test
  public void testIncludeJoinAndFetch() {
    // Ex: list all Artists and the price of each painting, even if the
    // Artist doesn’t have paintings.
    tm.transactionNoResult(db -> {
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

      assertEquals("Wrong size for outer join.", 4, values.size());
      for (ArtistValueDTO val : values) {
        assertNotNull("Invalid Name", val.getName());
        assertNotNull("Invalid Value", val.getValue());
        assertNotNull("Invalid Value.paintings", val.getPaintings());
      }
    });
  }

  @Test
  public void testJoinAndFetch() {
    tm.transactionNoResult(db -> {
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

      assertEquals("Wrong size for paintings.", 2, paintings.size());
      for (Painting paint : paintings) {
        assertNull("Artis should be null.", paint.getArtist());
      }
    });
  }

  @Test
  public void testSimpleFetch() {
    tm.transactionNoResult(db -> {
      Query query = db.query(TArtist.T_ARTIST).all()
          .orderBy(TArtist.C_NAME.asc())
          .outer(TArtist.A_PAINTINGS).fetch();
      List<Artist> artists = query.list(new MapTransformer<>(query, true, Artist.class, new ArtistDAOMapper()));

      dumpCollection(artists);

      assertEquals("Wrong size for artists. Expected 3, got " + artists.size(), 3, artists.size());
      Set<Painting> paintings = artists.get(0).getPaintings();
      assertNull("Wrong size for Paintings, for artist[0]. Expected null, got not null", paintings);
      int size = artists.get(1).getPaintings().size();
      assertEquals("Wrong size for Paintings, for artist[1]. Expected 2, got " + size, 2, size);
      size = artists.get(2).getPaintings().size();
      assertEquals("Wrong size for Paintings, for artist[2]. Expected 3, got " + size, 2, size);
    });
  }

  @Test
  public void testCustomFunction() {
    tm.transactionNoResult(db -> {
      // using custom functions
      Artist artist = db.query(TArtist.T_ARTIST)
          .column(FunctionExt.ifNull(TArtist.C_BIRTHDAY, new Date())).as("birthday")
          .where(TArtist.C_ID.is(1L))
          .unique(Artist.class);

      assertNotNull("Birthday is null when using custom function ifNull.", artist.getBirthday());
    });
  }

  @Test
  public void testTemporal() {
    tm.transactionNoResult(db -> {
      // INSERT
      Insert insert = db.insert(TTemporal.T_TEMPORAL).retrieveKeys(false)
          .sets(TTemporal.C_ID, TTemporal.C_CLOCK, TTemporal.C_TODAY, TTemporal.C_NOW, TTemporal.C_INSTANT);

      OffsetDateTime dateTime = OffsetDateTime.of(LocalDateTime.of(2019, 05, 25, 20, 1, 0),
          ZoneOffset.ofHoursMinutes(0, 0));
      Date date = Date.from(Instant.from(dateTime));
      MyTime myTime = new MyTime(date.getTime());
      MyDate myDate = new MyDate(date.getTime());
      MyDateTime myDateTime = new MyDateTime(date.getTime());
      insert.values(1L, myTime, myDate, myDateTime, date).execute();

      Temporal temporal = db.query(TTemporal.T_TEMPORAL).all()
          .where(TTemporal.C_ID.is(1L))
          .unique(Temporal.class);
      dump(temporal);

      SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.sssss");
      String expected = sdf.format(myTime);
      String actual = sdf.format(temporal.getClock());
      assertEquals("Clock is incorrect! Expected " + expected + ", got " + actual, myTime, temporal.getClock());

      sdf = new SimpleDateFormat("yyyy-MM-dd");
      assertEquals("Today is incorrect!", sdf.format(myDate), sdf.format(temporal.getToday()));

      sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      assertEquals("Now is incorrect!", sdf.format(myDateTime), sdf.format(temporal.getNow()));

      //sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
      assertEquals("Instant is incorrect!", sdf.format(date), sdf.format(temporal.getInstant()));
    });
  }

  @Test
  public void testCRUD() {
    tm.transactionNoResult(db -> {
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
    });
  }

  @Test
  public void testInsert() {
    tm.transactionNoResult(db -> {
      // INSERT
      Insert insert = db.insert(TArtist.T_ARTIST).retrieveKeys(false)
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
    });
  }

  @Test
  public void testQuickDelete() {
    tm.transactionNoResult(db -> {
      Painting painting = new Painting();
      painting.setId(4L);
      try {
        painting.setVersion(3); // wrong version
        db.delete(TPainting.T_PAINTING).submit(painting);
        fail("Version was not validated!");
      } catch (OptimisticLockException e) {
        assertTrue(true);
      }
      painting.setVersion(1); // wrong version
      Stopwatch sw = Stopwatch.createAndStart();
      boolean deleted = db.delete(TPainting.T_PAINTING).execute(painting);
      sw.stop().showTotal("SUBMIT DELETE").reset();
      assertTrue("Painting was not FAST deleted!", deleted);
    });
  }

  @Test
  public void testUpdateCache() {
    tm.transactionNoResult(db -> {
      Artist artist = db.query(TArtist.T_ARTIST).all()
          .where(TArtist.C_ID.is(1L)).unique(Artist.class);

      artist.setName("Jane Mnomonic");
      db.update(TArtist.T_ARTIST).submit(artist);
      assertEquals("Incorrect version!", 2, (int) artist.getVersion());

      artist.setGender(EGender.FEMALE);
      db.update(TArtist.T_ARTIST).submit(artist);
      assertEquals("Incorrect version!", 3, (int) artist.getVersion());
    });
  }

  @Test
  public void testQuickCRUD() {
    tm.transactionNoResult(db -> {
      // INSERT
      Artist artist = new Artist();
      artist.setName("John Mnomonic");
      artist.setGender(EGender.MALE);
      db.insert(TArtist.T_ARTIST).submit(artist);

      dump(artist);
      assertNotNull("No id returned when inserting!", artist.getId());
      assertSame("Generic Class for Id should be Long.class", artist.getId().getClass(), Long.class);

      List<Artist> artists = db.query(TArtist.T_ARTIST).all()
          .list(Artist.class);
      dumpCollection(artists);

      // UPDATE
      artist.setName("Jane Mnomonic");
      artist.setGender(EGender.FEMALE);
      db.update(TArtist.T_ARTIST).submit(artist);

      dump(artist);
      assertEquals("Incorrect version!", 1, (int) artist.getVersion());

      artists = db.query(TArtist.T_ARTIST).all()
          .list(Artist.class);
      dumpCollection(artists);
    });
  }

  @Test
  public void testBeanCRUD() {
    tm.transactionNoResult(db -> {
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
    });
  }

  @Test
  public void testInsertImage() {
    tm.transactionNoResult(db -> {
      BinStore bc = new BinStore();
      //  if file is not found
      bc.set(new File(RESOURCE_IMAGES + "/Starry_Night.jpg"));

      Insert insert = new Insert(db, TImage.T_IMAGE)
          .set(TImage.C_VERSION, 1)
          .set(TImage.C_CONTENT, bc);

      Map<Column<?>, Object> keys = insert.execute();
      for (Entry<Column<?>, Object> entry : keys.entrySet()) {
        assertNotNull(entry.getKey().getName() + " was null", entry.getValue());
      }
    });
  }

  @Test
  public void testLoadImage() {
    tm.transactionNoResult(db -> {
      List<BinStore> images = db.query(TImage.T_IMAGE)
          .column(TImage.C_CONTENT)
          .listRaw(BinStore.class);
      for (BinStore image : images) {
        System.out.println("size: " + image.get().length);
      }
      assertEquals("Wrong size for Image List!", 4, images.size());
    });
  }

  @Test
  public void testOnePaintingToOneImage() {
    tm.transactionNoResult(db -> {
      List<Painting> paintings = db.query(TPainting.T_PAINTING).all()
          .outerFetch(TPainting.A_IMAGE)
          .list(Painting.class);

      dumpCollection(paintings);

      assertEquals("Size of paintings list is wrong!", 4, paintings.size());
      Image image = paintings.get(0).getImage();
      assertNotNull("branch paintings[0].image was not returned.", image);
    });
  }

  @Test
  public void testOnePaintingToOneArtist() {
    tm.transactionNoResult(db -> {
      List<Painting> paintings = db.query(TPainting.T_PAINTING).all()
          .outerFetch(TPainting.A_ARTIST)
          .list(Painting.class);

      dumpCollection(paintings);

      assertEquals("Size of paintings list is wrong!", 4, paintings.size());
      Artist artist = paintings.get(0).getArtist();
      assertNotNull("branch paintings[0].artist was not returned.", artist);
    });
  }

  @Test
  public void testLoadImageBytes() {
    tm.transactionNoResult(db -> {
      List<ImageDTO> images = db.query(TImage.T_IMAGE).all()
          .list(ImageDTO.class);
      dumpCollection(images);
      for (ImageDTO image : images) {
        byte[] content = image.getContent();
        assertTrue("Image data is null!", content != null && content.length > 0);
      }
    });
  }

  @Test
  public void testInsertImageBytes() {
    tm.transactionNoResult(db -> {
      ImageDTO image = new ImageDTO();
      image.setVersion(1);
      image.setContent(BinStore.ofFile(RESOURCE_IMAGES + "/Starry_Night.jpg").get());
      Map<Column<?>, Object> keys = db.insert(TImage.T_IMAGE).set(image).execute();
      dump(image);

      Long key = (Long) keys.get(TImage.C_ID);
      image = db.query(TImage.T_IMAGE).all()
          .where(TImage.C_ID.is(key)).unique(ImageDTO.class);
      byte[] content = image.getContent();
      assertTrue("Image data is null!", content != null && content.length > 0);
    });
  }

  @Test
  public void testNumericEnum() {
    tm.transactionNoResult(db -> {
      Insert insert = db.insert(TEmployee.T_EMPLOYEE)
          .sets(TEmployee.C_ID, TEmployee.C_NAME, TEmployee.C_SEX, TEmployee.C_PAY_GRADE, TEmployee.C_CREATION);
      insert.values(1, "Oscar", true, EPayGrade.LOW, new Date()).execute();
      insert.values(2, "Maria", false, EPayGrade.HIGH, new Date()).execute();

      List<Employee> employees = db.query(TEmployee.T_EMPLOYEE).all()
          .orderBy(TEmployee.C_ID.asc())
          .list(Employee.class);

      dumpCollection(employees);

      assertEquals("Wrong list size for employees!", 2, employees.size());
      assertSame("Wrong pay grade type!", employees.get(0).getPayGrade(), EPayGrade.LOW);
      assertSame("Wrong pay grade type!", employees.get(1).getPayGrade(), EPayGrade.HIGH);
    });
  }

  @Test
  public void testSimpleCase() {
    tm.transactionNoResult(db -> {
      Long sale = db.query(TPainting.T_PAINTING)
          .column(
              sum(
                  with(TPainting.C_NAME)
                      .when("Blue Nude").then(10)
                      .otherwise(asIs(20)) // asIs(): value is written as is to the query
                      .end()
              )
          )
          .single(Long.class);

      assertEquals("Wrong sale value for Paintings! Expected 70, got " + sale, 70L, (long) sale);
    });
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
    tm.transactionNoResult(db -> {
      List<Classification> c = db.query(TPainting.T_PAINTING)
          .column(TPainting.C_NAME) // default maps to field name
          .column(
              when(TPainting.C_PRICE.gt(150D)).then("expensive")
                  .when(TPainting.C_PRICE.range(50D, 150D)).then("normal")
                  .otherwise("cheap")
                  .end()
          )
          .as("category") // maps to field category
          .orderBy(TPainting.C_PRICE.desc())
          .list(Classification.class);

      assertEquals("Wrong category value for Paintings!", "expensive", c.get(0).getCategory());
      assertEquals("Wrong category value for Paintings!", "normal", c.get(1).getCategory());
      assertEquals("Wrong category value for Paintings!", "normal", c.get(2).getCategory());
      assertEquals("Wrong category value for Paintings!", "cheap", c.get(3).getCategory());
    });
  }
}
