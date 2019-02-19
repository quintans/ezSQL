package com.github.quintans.ezSQL.orm;

import static com.github.quintans.ezSQL.orm.app.mappings.discriminator.TMain.T_MAIN;
import static com.github.quintans.ezSQL.orm.app.mappings.discriminator.TThing.T_THING;
import static com.github.quintans.ezSQL.orm.app.mappings.virtual.TEyeColor.T_EYE_COLOR;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.dml.Insert;
import com.github.quintans.ezSQL.dml.Update;
import com.github.quintans.ezSQL.orm.app.mappings.discriminator.Be;
import com.github.quintans.ezSQL.orm.app.mappings.discriminator.Main;
import com.github.quintans.ezSQL.orm.app.mappings.discriminator.TBe;
import com.github.quintans.ezSQL.orm.app.mappings.discriminator.TMain;
import com.github.quintans.ezSQL.orm.app.mappings.discriminator.TThing;
import com.github.quintans.ezSQL.orm.app.mappings.discriminator.Thing;
import com.github.quintans.ezSQL.orm.app.mappings.virtual.Catalog;
import com.github.quintans.ezSQL.orm.app.mappings.virtual.TGender;

/**
 * Unit test for simple App.
 */
public class TestDiscriminator extends TestBootstrap {

	@Test
	public void testDiscriminatorAssociation() {
		tm.readOnly(db -> {
			List<Main> mains = db.query(T_MAIN).all()
					.order(TMain.C_ID).asc()
					.outerFetch(TMain.A_BE)
					.outerFetch(TMain.A_CE)
					.list(Main.class);

			dumpCollection(mains);

			assertTrue("Wrong TMain list size", mains.size() == 3);
			Main m = mains.get(0);
			assertTrue("Wrong TMain childs", m.getBe() != null && m.getCe() == null);
			m = mains.get(1);
			assertTrue("Wrong TMain childs", m.getBe() != null && m.getCe() == null);
			m = mains.get(2);
			assertTrue("Wrong TMain childs", m.getBe() == null && m.getCe() != null);

			List<Be> bes = db.query(TBe.T_BE).all()
					.order(TBe.C_DSC).asc()
					.outerFetch(TBe.A_MAIN)
					.list(Be.class);
			dumpCollection(bes);

			assertTrue("Wrong TBe list size", bes.size() == 3);
			Be b = bes.get(0);
			assertTrue("Wrong TBe childs", b.getMains() != null);
			b = bes.get(1);
			assertTrue("Wrong TBe childs", b.getMains() != null);
			b = bes.get(2);
			assertTrue("Wrong TBe childs", b.getMains() == null);
		});
	}

    @Test
    public void testAssociationToTableDiscriminator() {
		tm.readOnly(db -> {
			List<Thing> things = db.query(T_THING).all()
					.order(TThing.C_ID).asc()
					.outer(TThing.A_TAA_B).fetch()
					.where(TThing.C_ID.is(1L))
					.list(Thing.class);

			dumpCollection(things);

			assertTrue("Wrong things list size", things.size() == 1);
			assertTrue("Wrong things.cenas list size", things.get(0).getCenas().size() == 1);
		});
    }

	@Test
	public void testQueryWithDiscriminatorColumn() {
		tm.readOnly(db -> {
			List<Catalog> genders = db.query(TGender.T_GENDER).all()
					.list(Catalog.class);

			dumpCollection(genders);
			assertTrue("Invalid list size for genders", genders.size() == 3);
		});
	}

    @Test
    public void testQueryWithDiscriminatorColumn2() {
		tm.readOnly(db -> {
			List<Catalog> eyeColors = db.query(T_EYE_COLOR).all()
					.where(T_EYE_COLOR.C_KEY.like("B%"))
					.list(Catalog.class);

			dumpCollection(eyeColors);
			assertTrue("Invalid list size for genders", eyeColors.size() == 2);
		});
    }

	@Test
	public void testInsertWithDiscriminatorColumn() {
		tm.transaction(db -> {
			Insert insert = db.insert(TGender.T_GENDER).sets(TGender.C_KEY, TGender.C_VALUE);
			Map<Column<?>, Object> keys = insert.values("H", "Hermafrodite").execute();

			assertTrue("Unable to insert with discriminator column", keys.get(TGender.C_ID) != null);
		});
	}

	@Test
	public void testUpdateWithDiscriminatorColumn() {
		tm.transaction(db -> {
			Update update = db.update(TGender.T_GENDER).sets(TGender.C_VALUE).where(TGender.C_KEY.is("U"));
			int result = update.values("Undefined").execute();

			assertTrue("Unable to update with discriminator column", result == 1);
		});
	}

	@Test
	public void testUpdateAllWithDiscriminatorColumn() {
		tm.transaction(db -> {
			Update update = db.update(TGender.T_GENDER).sets(TGender.C_VALUE);
			int result = update.values("Undefined").execute();

			assertTrue("Unable to update with discriminator column", result == 3);
		});
	}

	@Test
	public void testDeleteWithDiscriminatorColumn() {
		tm.transaction(db -> {
			int result = db.delete(T_EYE_COLOR).where(T_EYE_COLOR.C_KEY.like("B%")).execute();

			assertTrue("Unable to update with discriminator column", result == 2);
		});
	}


	@Test
	public void testDeleteAllWithDiscriminatorColumn() {
		tm.transaction(db -> {
			int result = db.delete(T_EYE_COLOR).execute();

			assertTrue("Unable to update with discriminator column", result == 3);
		});
	}
}
