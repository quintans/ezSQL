package com.github.quintans.ezSQL.toolkit.utils;

import com.github.quintans.ezSQL.toolkit.io.BinStore;
import com.github.quintans.jdbc.exceptions.PersistenceException;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

public class Misc {
    public static boolean match(Object o1, Object o2) {
        if (o1 == o2) // even if both are null
            return true;
        else if (o1 != null)
            return o1.equals(o2);
        else
            return o2.equals(o1);
    }

    public static int length(Collection<?> coll) {
        return coll == null ? 0 : coll.size();
    }

    public static int length(Object[] data) {
        return data == null ? 0 : data.length;
    }


    public static boolean empty(Collection<?> coll) {
        return length(coll) == 0;
    }

    public static boolean empty(Object[] data) {
        return length(data) == 0;
    }


    public static void copy(InputStream in, BinStore bc) {
        if (in == null)
            return;

        try {
            bc.set(in);
        } catch (IOException e) {
            throw new PersistenceException("Unable to set stream into bytecache!", e);
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

}
