package com.github.quintans.ezSQL.mapper;

public interface Row {
  Object get(int columnIndex);
  <T> T get(int columnIndex, Class<T> type);
}
