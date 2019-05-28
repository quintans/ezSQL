package com.github.quintans.ezSQL.orm.app.daos;

import com.github.quintans.ezSQL.orm.app.domain.Artist;
import com.github.quintans.ezSQL.orm.app.domain.Painting;
import com.github.quintans.ezSQL.orm.app.mappings.TPainting;
import com.github.quintans.ezSQL.mapper.MapColumn;
import com.github.quintans.ezSQL.mapper.QueryMapper;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import com.github.quintans.ezSQL.mapper.Row;

import java.util.List;

public class PaintingDAOMapper implements QueryMapper {
  private QueryMapper artistMapper;

  private QueryMapper getArtistMapper() {
    if (artistMapper == null) {
      artistMapper = new ArtistDAOMapper();
    }
    return artistMapper;
  }

  @Override
  public boolean support(Class<?> rootClass) {
    return Painting.class.equals(rootClass);
  }

  @Override
  public Object createRoot(Class<?> rootClass) {
    return new Painting();
  }

  @Override
  public Object createFrom(Object parentInstance, String name) {
    if (parentInstance instanceof Painting) {
      if (TPainting.A_ARTIST.getAlias().equals(name)) {
        return getArtistMapper().createRoot(Artist.class);
      }
    }

    throw new IllegalArgumentException("Unknown mapping for alias " +
        parentInstance.getClass().getCanonicalName() + "#" + name);

  }

  @Override
  public void link(Object instance, String name, Object value) {
    if (instance instanceof Painting) {
      Painting entity = (Painting) instance;

      if (TPainting.A_ARTIST.getAlias().equals(name)) {
        entity.setArtist((Artist) value);
      }
    } else if (instance instanceof Painting) {
      getArtistMapper().link(instance, name, value);
    }
  }

  @Override
  public boolean map(Row row, Object instance, List<MapColumn> mapColumns) {
    try {
      boolean touched = false;

      if (instance instanceof Painting) {
        Painting entity = (Painting) instance;
        for (MapColumn mapColumn : mapColumns) {
          int idx = mapColumn.getIndex();
          String alias = mapColumn.getAlias();

          if (TPainting.C_ID.getAlias().equals(alias)) {
            Long value = row.get(idx, Long.class);
            entity.setId(value);
            touched |= value != null;
          } else if (TPainting.C_VERSION.getAlias().equals(alias)) {
            Integer value = row.get(idx, Integer.class);
            entity.setVersion(value);
            touched |= value != null;
          } else if (TPainting.C_NAME.getAlias().equals(alias)) {
            String value = row.get(idx, String.class);
            entity.setName(value);
            touched |= value != null;
          } else if (TPainting.C_PRICE.getAlias().equals(alias)) {
            Double value = row.get(idx, Double.class);
            entity.setPrice(value);
            touched |= value != null;
          }
        }
      } else if (instance instanceof Artist) {
        touched = getArtistMapper().map(row, instance, mapColumns);
      }
      return touched;
    } catch (Exception e) {
      throw new PersistenceException(e);
    }
  }
}
