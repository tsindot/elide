package com.yahoo.elide.datastores.aggregation.queryengines.sql.metadata;

import static com.yahoo.elide.datastores.aggregation.metadata.ColumnVisitor.resolveFormulaReferences;
import lombok.AllArgsConstructor;
import lombok.Data;

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
