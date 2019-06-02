package com.github.quintans.ezSQL.mapper;

import com.github.quintans.ezSQL.db.Column;
import com.github.quintans.ezSQL.translator.Translator;
import com.github.quintans.ezSQL.toolkit.utils.Result;

public interface UpdateMapper extends MapperSupporter {
    Result<UpdateValue> map(Translator translator, Column column, Object object);
    Object newVersion(Object version);
}
