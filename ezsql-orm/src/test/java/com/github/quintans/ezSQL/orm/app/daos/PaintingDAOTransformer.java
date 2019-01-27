package com.github.quintans.ezSQL.orm.app.daos;

import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.orm.app.domain.Artist;
import com.github.quintans.ezSQL.orm.app.domain.Painting;
import com.github.quintans.ezSQL.orm.app.mappings.TPainting;
import com.github.quintans.ezSQL.transformers.MapColumn;
import com.github.quintans.ezSQL.transformers.Mapper;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;

public class PaintingDAOTransformer implements Mapper {
    private Driver driver;
    private Mapper artistMapper;

    public PaintingDAOTransformer(Driver driver) {
        this.driver = driver;
    }

    private Mapper getArtistMapper() {
        if(artistMapper == null) {
            artistMapper = new ArtistDAOTransformer(driver);
        }
        return artistMapper;
    }

    @Override
    public Object createFrom(Object parentInstance, String name) {
        if (parentInstance instanceof Artist) {
            if (TPainting.A_ARTIST.getAlias().equals(name)) {
                return getArtistMapper().createFrom(parentInstance, name);
            }
        }

        return new Painting();
    }

    @Override
    public void apply(Object instance, String name, Object value) {
        if (instance instanceof Painting) {
            Painting entity = (Painting) instance;

            if (TPainting.A_ARTIST.getAlias().equals(name)) {
                entity.setArtist((Artist) value);
            }
        } else if (instance instanceof Painting) {
            getArtistMapper().apply(instance, name, value);
        }
    }

    @Override
    public Object map(ResultSetWrapper rsw, Object instance, MapColumn mapColumn) {
        try {
            int idx = mapColumn.getColumnIndex();
            String alias = mapColumn.getAlias();

            if (instance instanceof Painting) {
                Painting entity = (Painting) instance;

                if (TPainting.C_ID.getAlias().equals(alias)) {
                    Long value = driver.fromDb(rsw, idx, Long.class);
                    entity.setId(value);
                    return value;
                } else if (TPainting.C_VERSION.getAlias().equals(alias)) {
                    Integer value = driver.fromDb(rsw, idx, Integer.class);
                    entity.setVersion(value);
                    return value;
                } else if (TPainting.C_NAME.getAlias().equals(alias)) {
                    String value = driver.fromDb(rsw, idx, String.class);
                    entity.setName(value);
                    return value;
                } else if (TPainting.C_PRICE.getAlias().equals(alias)) {
                    Double value = driver.fromDb(rsw, idx, Double.class);
                    entity.setPrice(value);
                    return value;
                }
            } else if (instance instanceof Painting) {
                return getArtistMapper().map(rsw, instance, mapColumn);
            }
            return null;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }
}
