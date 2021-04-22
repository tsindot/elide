package com.yahoo.elide.core.security.visitors;

import com.yahoo.elide.core.security.checks.Check;
import com.yahoo.elide.core.security.checks.DatastoreEvalFilterExpressionCheck;
import com.yahoo.elide.core.security.checks.OperationCheck;
import com.yahoo.elide.core.security.permissions.expressions.AndExpression;
import com.yahoo.elide.core.security.permissions.expressions.CheckExpression;
import com.yahoo.elide.core.security.permissions.expressions.Expression;
import com.yahoo.elide.core.security.permissions.expressions.ExpressionVisitor;
import com.yahoo.elide.core.security.permissions.expressions.NotExpression;
import com.yahoo.elide.core.security.permissions.expressions.OrExpression;

public class ExecuteFilterCheckInMemoryVisitor implements ExpressionVisitor<Expression> {

    @Override
    public Expression visitExpression(Expression expression) {
        return expression;
    }

    @Override
    public Expression visitCheckExpression(CheckExpression checkExpression) {
        Check check = checkExpression.getCheck();
        if (check instanceof DatastoreEvalFilterExpressionCheck) {
            ((DatastoreEvalFilterExpressionCheck) check).setExecutedInMemory(true);
        }
        return checkExpression;
    }

    @Override
    public Expression visitAndExpression(AndExpression andExpression) {
        Expression left = andExpression.getLeft().accept(this);
        Expression right = andExpression.getRight().accept(this);
        return andExpression;
    }

    @Override
    public Expression visitOrExpression(OrExpression orExpression) {
        Expression left = orExpression.getLeft().accept(this);
        Expression right = orExpression.getRight().accept(this);
        return orExpression;
    }

    @Override
    public Expression visitNotExpression(NotExpression notExpression) {
        return notExpression.getLogical().accept(this);
    }
}
