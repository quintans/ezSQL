package com.github.quintans.ezSQL.toolkit.utils;

import com.github.quintans.ezSQL.exception.OrmException;
import com.github.quintans.ezSQL.toolkit.io.BinStore;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

public class Misc {
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
      throw new OrmException("Unable to set stream into bytecache!", e);
    } finally {
      IOUtils.closeQuietly(in);
    }
  }

}
