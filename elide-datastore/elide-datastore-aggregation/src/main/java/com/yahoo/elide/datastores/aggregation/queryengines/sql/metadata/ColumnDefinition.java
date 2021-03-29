/*
 * Copyright 2021, Yahoo Inc.
 * Licensed under the Apache License, Version 2.0
 * See LICENSE file in project root for terms.
 */
package com.yahoo.elide.datastores.aggregation.queryengines.sql.metadata;

import static com.yahoo.elide.datastores.aggregation.metadata.ColumnVisitor.resolveFormulaReferences;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Column Definition for Handlebars Resolution.
 */
@Data
@AllArgsConstructor
public class ColumnDefinition {
    String definition;
    String path;

    @Override
    public String toString() {

        String expr = definition;
        for (String reference : resolveFormulaReferences(expr)) {
            // ToDo More Strict Replacement required
            String updatedReference = reference;
            if (!path.isEmpty() && !reference.startsWith("$$")) {
                updatedReference = path + "." + reference;
                expr = expr.replace(reference, updatedReference);
            }

        }

        return expr;
    }
}
