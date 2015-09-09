package pt.quintans.ezSQL.orm.app.daos;

import static pt.quintans.ezSQL.orm.app.mappings.TPainting.A_ARTIST;
import static pt.quintans.ezSQL.orm.app.mappings.TPainting.C_ID;
import static pt.quintans.ezSQL.orm.app.mappings.TPainting.C_NAME;
import static pt.quintans.ezSQL.orm.app.mappings.TPainting.C_PRICE;
import static pt.quintans.ezSQL.orm.app.mappings.TPainting.C_VERSION;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import pt.quintans.ezSQL.db.Association;
import pt.quintans.ezSQL.dml.Query;
import pt.quintans.ezSQL.orm.app.domain.Artist;
import pt.quintans.ezSQL.orm.app.domain.Painting;
import pt.quintans.ezSQL.orm.domain.IORMRowTransformerFactory;
import pt.quintans.ezSQL.orm.domain.ORMTransformer;
import pt.quintans.ezSQL.orm.domain.SimpleEntityCache;
import pt.quintans.ezSQL.transformers.IRowTransformer;

public class PaintingDAOBase implements IPaintingDAO, IORMRowTransformerFactory<Painting> {

	public static final IORMRowTransformerFactory<Painting> factory = new PaintingDAOBase();

	@Override
	public IRowTransformer<Painting> createTransformer(Query query, boolean reuse) {
		return new Transformer(query, reuse);
	}

	@Override
	public IRowTransformer<Painting> createTransformer(Association foreignKey, IRowTransformer<?> other) {
		return new Transformer(foreignKey, (ORMTransformer<?>) other);
	}

	public static class Transformer extends ORMTransformer<Painting> {
		public Transformer(Query query, boolean reuse) {
			super(query, reuse);
		}

		public Transformer(Association foreignKey, ORMTransformer<?> other) {
			super(foreignKey, other);
		}

		@Override
		public Painting transform(ResultSet rs, int[] columnTypes) throws SQLException {
			// in a outer join, an entity can have null for all of its fields, even for the id
			Long id =  getLong(C_ID);
			if (id == null)
				return null;

			Painting temp = new Painting();
			temp.setId(id);

			Painting painting = null;

			if (isReuse())
				painting = SimpleEntityCache.cache(temp);

			if (painting == null) {
				painting = temp;

				painting.setVersion(getInteger(C_VERSION));
				painting.setName(getString(C_NAME));
				painting.setPrice(getDecimal(C_PRICE));
			}

			// obtains the associations for this level
			List<Association> fks = forwardBranches();
			// checks to see witch entity is to be loaded
			if (fks != null) {
				for (Association fk : fks) {
					if (A_ARTIST.getAlias().equals(fk.getAlias()))
						loadArtist(fk, painting);
				}
			}

			return painting;
		}

		private void loadArtist(Association foreignKey, Painting entity) throws SQLException {
			Artist artist = loadEntity(foreignKey, ArtistDAOBase.factory);

			if (artist != null)
				entity.setArtist(artist);
		}
	}

}
