package pt.quintans.ezSQL.orm.app.daos;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.List;

import pt.quintans.ezSQL.db.Association;
import pt.quintans.ezSQL.dml.Query;
import pt.quintans.ezSQL.orm.app.domain.Artist;
import pt.quintans.ezSQL.orm.app.domain.Painting;
import pt.quintans.ezSQL.orm.app.mappings.TArtist;
import pt.quintans.ezSQL.orm.domain.IORMRowTransformerFactory;
import pt.quintans.ezSQL.orm.domain.ORMTransformer;
import pt.quintans.ezSQL.orm.domain.SimpleEntityCache;
import pt.quintans.ezSQL.transformers.IQueryRowTransformer;
import pt.quintans.ezSQL.transformers.IRowTransformer;

public class ArtistDAOBase implements IArtistDAO, IORMRowTransformerFactory<Artist> {

	public static final IORMRowTransformerFactory<Artist> factory = new ArtistDAOBase();

	@Override
	public IQueryRowTransformer<Artist> createTransformer(Query query, boolean reuse) {
		return new Transformer(query, reuse);
	}

	@Override
	public IQueryRowTransformer<Artist> createTransformer(Association foreignKey, IRowTransformer<?> other) {
		return new Transformer(foreignKey, (ORMTransformer<?>) other);
	}

	public static class Transformer extends ORMTransformer<Artist> {
		public Transformer(Query query, boolean reuse) {
			super(query, reuse);
		}

		public Transformer(Association foreignKey, ORMTransformer<?> other) {
			super(foreignKey, other);
		}

		@Override
		public Artist transform(ResultSet rs, int[] columnTypes) throws SQLException {
			// in a outer join, an entity can have null for all of its fields, even for the id
			Long id = getLong(TArtist.C_ID);
			if (id == null)
				return null;

			Artist temp = new Artist();
			temp.setId(id);

			Artist artist = null;

			if (isReuse())
				artist = SimpleEntityCache.cache(temp);

			if (artist == null) {
				artist = temp;

				artist.setVersion(getInteger(TArtist.C_VERSION));
				artist.setName(getString(TArtist.C_NAME));
			}

			// obtains the associations for this level
			List<Association> fks = forwardBranches();
			// checks to see witch entity is to be loaded
			if (fks != null) {
				for (Association fk : fks) {
					if (TArtist.A_PAINTINGS.getAlias().equals(fk.getAlias()))
						loadPaintings(fk, artist);
				}
			}

			return artist;
		}

		private void loadPaintings(Association foreignKey, Artist entity) throws SQLException {
			// garanto que já fica definido, evitando lazy loading em futuras chamadas
			if (entity.getPaintings() == null)
				entity.setPaintings(new LinkedHashSet<Painting>());

			Painting painting = loadEntity(foreignKey, PaintingDAOBase.factory);

			if (painting != null)
				// não monta em árvore ou ainda não foi adicionado
				if (!isReuse() || !entity.getPaintings().contains(painting))
					entity.getPaintings().add(painting);
		}

	}
}
