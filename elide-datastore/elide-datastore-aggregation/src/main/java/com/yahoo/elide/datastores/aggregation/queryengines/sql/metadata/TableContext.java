package com.yahoo.elide.datastores.aggregation.queryengines.sql.metadata;

import java.util.HashMap;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class TableContext extends HashMap<String, Object> {

    String alias;

    public Object get(Object key) {
        if (key.toString().lastIndexOf('$') == 0) {
            return alias + "." + key.toString().substring(1);
        }
        Object value = super.get(key);

        return value;
    }
}
