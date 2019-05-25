package com.github.quintans.ezSQL.transformers;

import com.github.quintans.ezSQL.AbstractDb;
import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.toolkit.utils.Result;

public interface UpdateMapper extends MapperSupporter {
    Result<UpdateValue> map(AbstractDb db, Column column, Object object);
    Object newVersion(Object version);
}
