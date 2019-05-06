package com.github.quintans.ezSQL.transformers;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.toolkit.utils.Result;

public interface DeleteMapper extends MapperSupporter {
    Result<Object> map(Column column, Object object);
}
