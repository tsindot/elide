/*
 * Copyright 2020, Yahoo Inc.
 * Licensed under the Apache License, Version 2.0
 * See LICENSE file in project root for terms.
 */

package com.yahoo.elide.datastores.aggregation.queryengines.sql.query;

import com.yahoo.elide.datastores.aggregation.metadata.MetaDataStore;
import com.yahoo.elide.datastores.aggregation.metadata.TableContext;
import com.yahoo.elide.datastores.aggregation.query.ColumnProjection;
import com.yahoo.elide.datastores.aggregation.query.Queryable;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.calcite.SyntaxVerifier;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.dialects.SQLDialect;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.expression.ExpressionParser;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.expression.HasJoinVisitor;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.expression.PhysicalReferenceExtractor;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.expression.Reference;
import com.yahoo.elide.datastores.aggregation.queryengines.sql.metadata.SQLReferenceTable;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Column projection that can expand the column into a SQL projection fragment.
 */
public interface SQLColumnProjection extends ColumnProjection {
    public static Logger LOGGER = LoggerFactory.getLogger(SQLColumnProjection.class);

    /**
     * Generate a SQL fragment for this combination column and client arguments.
     * @param query current Queryable.
     * @param lookupTable symbol table to resolve column name references.
     * @return SQL query String for this column
     */
    default String toSQL(Queryable query, SQLReferenceTable lookupTable) {

        TableContext tableCtx = lookupTable.getGlobalTableContext(query);

        return tableCtx.resolveHandlebars(getName(), getExpression(), getArguments());
    }

    @Override
    default boolean canNest(Queryable source, SQLReferenceTable lookupTable) {
        SQLDialect dialect = source.getConnectionDetails().getDialect();
        String sql = toSQL(source, lookupTable);

        SyntaxVerifier verifier = new SyntaxVerifier(dialect);
        boolean canNest = verifier.verify(sql);
        if (! canNest) {
            LOGGER.debug("Unable to nest {} because {}", this.getName(), verifier.getLastError());
        }

        return canNest;
    }

    @Override
    default Pair<ColumnProjection, Set<ColumnProjection>> nest(Queryable source,
                                                              SQLReferenceTable lookupTable,
                                                              boolean joinInOuter) {

        MetaDataStore store = lookupTable.getMetaDataStore();
        List<Reference> references = new ExpressionParser(store).parse(source, getExpression());

        boolean requiresJoin = requiresJoin(references);

        boolean inProjection = source.getColumnProjection(getName(), getArguments()) != null;

        ColumnProjection outerProjection;
        Set<ColumnProjection> innerProjections;

        if (requiresJoin && joinInOuter) {

            //TODO - the expression needs to be rewritten to leverage the inner column physical projections.
            outerProjection = withExpression(getExpression(), inProjection);

            innerProjections = extractPhysicalReferences(references, store);
        } else {
            outerProjection = withExpression("{{$" + this.getSafeAlias() + "}}", isProjected());
            innerProjections = new LinkedHashSet<>(Arrays.asList(this));
        }

        return Pair.of(outerProjection, innerProjections);
    }

    SQLColumnProjection withExpression(String expression, boolean project);

    /**
     * Returns whether or not this column is projected in the output (included in SELECT) or
     * only referenced in a filter expression.
     * @return True if part of the output projection.  False otherwise.
     */
    default boolean isProjected() {
        return true;
    }

    /**
     * Determines if a particular column projection requires a join to another table.
     * @param source Source table.
     * @param projection The column.
     * @param store The metadata store.
     * @return True if the column requires a join.  False otherwise.
     */
    static boolean requiresJoin(Queryable source, ColumnProjection projection, MetaDataStore store) {
        List<Reference> references = new ExpressionParser(store).parse(source, projection.getExpression());
        return requiresJoin(references);
    }

    /**
     * Determines if a join is required amongst a list of column references.
     * @param references The list of references.
     * @return True if a join is required.  False otherwise.
     */
    static boolean requiresJoin(List<Reference> references) {
        return references.stream().anyMatch(ref -> ref.accept(new HasJoinVisitor()));
    }

    /**
     * Extracts all of the physical column projections that are referenced in a list of references.
     * @param references The list of references.
     * @param store The MetaDataStore.
     * @return A set of physical column projections.
     */
    static Set<ColumnProjection> extractPhysicalReferences(List<Reference> references, MetaDataStore store) {
        return references.stream()
                .map(ref -> ref.accept(new PhysicalReferenceExtractor(store)))
                .flatMap(Set::stream)
                .map(ref -> SQLPhysicalColumnProjection.builder()
                        .name(ref.getName())
                        .build())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }
}
