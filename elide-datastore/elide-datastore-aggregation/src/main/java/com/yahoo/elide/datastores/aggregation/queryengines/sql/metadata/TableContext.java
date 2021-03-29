/*
 * Copyright 2021, Yahoo Inc.
 * Licensed under the Apache License, Version 2.0
 * See LICENSE file in project root for terms.
 */
package com.yahoo.elide.datastores.aggregation.queryengines.sql.metadata;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.HashMap;

/**
 * TableContext for Handlebars Resolution.
 */
@AllArgsConstructor
@Getter
public class TableContext extends HashMap<String, Object> {

    private static final long serialVersionUID = -2307763404213383909L;

    String alias;

    public Object get(Object key) {
        if (key.toString().lastIndexOf('$') == 0) {
            return alias + "." + key.toString().substring(1);
        }
        Object value = super.get(key);

        return value;
    }
}
