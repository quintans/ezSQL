package com.github.quintans.ezSQL.mapper;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.toolkit.utils.Result;

public interface InsertMapper extends MapperSupporter {
  Result<Object> map(Driver driver, Column<?> column, Object object, boolean versioned);
}
