package com.github.quintans.ezSQL.mapper;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.driver.Driver;
import com.github.quintans.ezSQL.toolkit.utils.Result;

public interface UpdateMapper extends MapperSupporter {
    Result<UpdateValue> map(Driver driver, Column column, Object object);
    Object newVersion(Object version);
}
