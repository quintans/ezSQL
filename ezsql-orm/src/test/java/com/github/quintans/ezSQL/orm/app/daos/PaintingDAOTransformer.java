package com.github.quintans.ezSQL.orm.app.daos;

import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.orm.app.domain.Artist;
import com.github.quintans.ezSQL.orm.app.domain.Painting;
import com.github.quintans.ezSQL.orm.app.mappings.TPainting;
import com.github.quintans.ezSQL.transformers.MapColumn;
import com.github.quintans.ezSQL.transformers.QueryMapper;
import com.github.quintans.ezSQL.transformers.Record;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;

import java.util.List;

public class PaintingDAOTransformer implements QueryMapper<Painting> {
    private Driver driver;
    private QueryMapper artistMapper;

    public PaintingDAOTransformer(Driver driver) {
        this.driver = driver;
    }

    private QueryMapper getArtistMapper() {
        if (artistMapper == null) {
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
    public boolean map(Record record, Object instance, List<MapColumn> mapColumns) {
        try {
            boolean touched = false;

            if (instance instanceof Painting) {
                Painting entity = (Painting) instance;
                for (MapColumn mapColumn : mapColumns) {
                    int idx = mapColumn.getIndex();
                    String alias = mapColumn.getAlias();

                    if (TPainting.C_ID.getAlias().equals(alias)) {
                        Long value = record.get(idx, Long.class);
                        entity.setId(value);
                        touched |= value != null;
                    } else if (TPainting.C_VERSION.getAlias().equals(alias)) {
                        Integer value = record.get(idx, Integer.class);
                        entity.setVersion(value);
                        touched |= value != null;
                    } else if (TPainting.C_NAME.getAlias().equals(alias)) {
                        String value = record.get(idx, String.class);
                        entity.setName(value);
                        touched |= value != null;
                    } else if (TPainting.C_PRICE.getAlias().equals(alias)) {
                        Double value = record.get(idx, Double.class);
                        entity.setPrice(value);
                        touched |= value != null;
                    }
                }
            } else if (instance instanceof Painting) {
                touched = getArtistMapper().map(record, instance, mapColumns);
            }
            return touched;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }
}
