package com.github.quintans.ezSQL.orm.app.daos;

import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.orm.app.domain.Artist;
import com.github.quintans.ezSQL.orm.app.domain.Painting;
import com.github.quintans.ezSQL.orm.app.mappings.TArtist;
import com.github.quintans.ezSQL.transformers.MapColumn;
import com.github.quintans.ezSQL.transformers.Mapper;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import com.github.quintans.jdbc.transformers.ResultSetWrapper;

import java.util.LinkedHashSet;
import java.util.List;

public class ArtistDAOTransformer implements Mapper {
    private Driver driver;
    private Mapper paintingMapper;

    public ArtistDAOTransformer(Driver driver) {
        this.driver = driver;
    }


    private Mapper getPaintingMapper() {
        if (paintingMapper == null) {
            paintingMapper = new PaintingDAOTransformer(driver);
        }
        return paintingMapper;
    }

    @Override
    public Object createFrom(Object parentInstance, String name) {
        if (parentInstance instanceof Artist) {
            if (TArtist.A_PAINTINGS.getAlias().equals(name)) {
                return getPaintingMapper().createFrom(parentInstance, name);
            }
        }

        return new Artist();
    }

    @Override
    public void apply(Object instance, String name, Object value) {
        if (instance instanceof Artist) {
            Artist entity = (Artist) instance;

            if (TArtist.A_PAINTINGS.getAlias().equals(name)) {
                if (entity.getPaintings() == null) {
                    entity.setPaintings(new LinkedHashSet<>());
                }
                entity.getPaintings().add((Painting) value);
            }
        } else if (instance instanceof Painting) {
            getPaintingMapper().apply(instance, name, value);
        }
    }

    @Override
    public boolean map(ResultSetWrapper rsw, Object instance, List<MapColumn> mapColumns) {
        try {
            boolean touched = false;

            if (instance instanceof Artist) {
                Artist entity = (Artist) instance;

                for (MapColumn mapColumn : mapColumns) {
                    int idx = mapColumn.getColumnIndex();
                    String alias = mapColumn.getAlias();

                    if (TArtist.C_ID.getAlias().equals(alias)) {
                        Long value = driver.fromDb(rsw, idx, Long.class);
                        entity.setId(value);
                        touched |= value != null;
                    } else if (TArtist.C_VERSION.getAlias().equals(alias)) {
                        Integer value = driver.fromDb(rsw, idx, Integer.class);
                        entity.setVersion(value);
                        touched |= value != null;
                    } else if (TArtist.C_NAME.getAlias().equals(alias)) {
                        String value = driver.fromDb(rsw, idx, String.class);
                        entity.setName(value);
                        touched |= value != null;
                    }
                }
            } else if (instance instanceof Painting) {
                touched = getPaintingMapper().map(rsw, instance, mapColumns);
            }
            return touched;

        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }
}
