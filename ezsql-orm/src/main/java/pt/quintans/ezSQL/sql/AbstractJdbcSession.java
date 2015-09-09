package pt.quintans.ezSQL.sql;

import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import pt.quintans.ezSQL.exceptions.PersistenceException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public abstract class AbstractJdbcSession implements JdbcSession {
    /**
     * Is {@link ParameterMetaData#getParameterType(int)} broken (have we tried it yet)?
     */
    private boolean pmdKnownBroken = false;
    

    @Override
    public abstract Connection getConnection();

    @Override
    public boolean getPmdKnownBroken() {
        return pmdKnownBroken;
    }

    @Override
    public void setPmdKnownBroken(boolean pmdKnownBroken) {
        this.pmdKnownBroken = pmdKnownBroken;
    }

    /*
     * to avoid collision that may occur if the same SQL is used in different databases/schemas, 
     * the conncetion used must allways target the same database/schema.
     */
    private final Cache<String, int[]> columnTypeCache = CacheBuilder.newBuilder()
        .maximumSize(1000)
        .build();

    @Override
    public int[] fetchColumnTypesForSelect(final String sql, final ResultSet resultSet) {
        String select = fetchSelect(sql);
        int[] columnTypes = null;
        try {
            columnTypes = columnTypeCache.get(select, new Callable<int[]>() {
                @Override
                public int[] call() {
                    int[] types = null;
                    try {
                        ResultSetMetaData rsmd = resultSet.getMetaData();
                        int cols = rsmd.getColumnCount();
                        types = new int[cols];
                        for (int i = 0; i < types.length; i++) {
                            types[i] = rsmd.getColumnType(i + 1);
                        }
                    } catch (SQLException e) {
                        throw new PersistenceException("Unable to get ResultSetMetaData for " + sql, e);
                    }
                    return types;
                }
            });
        } catch (ExecutionException e) {
            throw new PersistenceException("Unable to get column types for " + sql, e);
        }

        return columnTypes;
    }

    private static final char[] FROM = " FROM ".toCharArray();
    private static final char PELICA = '\'';

    private static String fetchSelect(String sql) {
        char[] SQL = sql.toUpperCase().toCharArray();
        boolean outside = true;
        // holds the current position of the 'FROM' matching
        int posFrom = 0;
        int deep = 0;
        // traverse all letters of the SQL
        for (int i = 0; i < SQL.length; i++) {
            char letter = SQL[i];
            // 'FROM' inside text is ignored
            if (letter == PELICA)
                outside = !outside;
            // ignore blank characters
            else if (letter == '\r' || letter == '\n' || letter == '\t')
                letter = ' ';

            if (outside) {
                if (letter == '(')
                    deep++;
                else if (letter == ')')
                    deep--;

                if (deep == 0) {
                    // advances the matching cursor or resets it
                    posFrom = (letter == FROM[posFrom] ? posFrom + 1 : 0);
                    // if the cursor is at the end of the word, it was fully matched
                    if (posFrom == FROM.length) {
                        // found
                        return sql.substring(0, i - FROM.length + 1);
                    }
                }
                else
                    posFrom = 0;
            }
            else
                posFrom = 0;
        }

        throw new RuntimeException("The 'FROM' clause was not found in " + sql);
    }


}
