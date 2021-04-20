package com.yahoo.elide.core.security.visitors;

import com.yahoo.elide.core.dictionary.EntityDictionary;
import com.yahoo.elide.core.security.CheckInstantiator;
import com.yahoo.elide.core.security.RequestScope;
import com.yahoo.elide.core.security.checks.Check;
import com.yahoo.elide.core.security.checks.FilterExpressionCheck;
import com.yahoo.elide.core.security.checks.UserCheck;
import com.yahoo.elide.core.security.permissions.ExpressionResult;
import com.yahoo.elide.core.security.permissions.expressions.AndExpression;
import com.yahoo.elide.core.security.permissions.expressions.CheckExpression;
import com.yahoo.elide.core.security.permissions.expressions.Expression;
import com.yahoo.elide.core.security.permissions.expressions.NotExpression;
import com.yahoo.elide.core.security.permissions.expressions.OrExpression;
import com.yahoo.elide.generated.parsers.ExpressionBaseVisitor;
import com.yahoo.elide.generated.parsers.ExpressionParser;

import lombok.AllArgsConstructor;

import java.util.Objects;
import java.util.function.Function;

@AllArgsConstructor
public class InmemoryPermissionExpressionVisitor extends ExpressionBaseVisitor<Expression>
        implements CheckInstantiator {
    private final EntityDictionary dictionary;
    private final RequestScope requestScope;
    private final Function<Check, Expression> expressionGenerator;

    public static final Expression TRUE_EXPRESSION = mode -> ExpressionResult.PASS;

    public static final Expression FALSE_EXPRESSION = mode -> ExpressionResult.FAIL;

    public static final Expression NOOP_EXPRESSION = mode -> ExpressionResult.PASS;


    @Override
    public Expression visitNOT(ExpressionParser.NOTContext ctx) {
        Expression expression = visit(ctx.expression());
        if (Objects.equals(expression, TRUE_EXPRESSION)) {
            return FALSE_EXPRESSION;
        }
        if (Objects.equals(expression, FALSE_EXPRESSION)) {
            return TRUE_EXPRESSION;
        }
        if (Objects.equals(expression, NOOP_EXPRESSION)) {
            return NOOP_EXPRESSION;
        }
        return new NotExpression(expression);
    }

    @Override
    public Expression visitOR(ExpressionParser.ORContext ctx) {
        Expression left = visit(ctx.left);
        Expression right = visit(ctx.right);
        if (Objects.equals(left, NOOP_EXPRESSION) || Objects.equals(left, TRUE_EXPRESSION)) {
            return left;
        }
        if (Objects.equals(right, NOOP_EXPRESSION) || Objects.equals(right, TRUE_EXPRESSION)) {
            return right;
        }

        if (Objects.equals(left, FALSE_EXPRESSION)) {
            return right;
        }
        if (Objects.equals(left, FALSE_EXPRESSION)) {
            return left;
        }

        return new OrExpression(left, right);
    }

    @Override
    public Expression visitAND(ExpressionParser.ANDContext ctx) {
        Expression left = visit(ctx.left);
        Expression right = visit(ctx.right);

        if (Objects.equals(left, FALSE_EXPRESSION) || Objects.equals(right, FALSE_EXPRESSION)) {
            return FALSE_EXPRESSION;
        }

        if (Objects.equals(left, NOOP_EXPRESSION) || Objects.equals(left, TRUE_EXPRESSION)) {
            return right;
        }
        if (Objects.equals(right, NOOP_EXPRESSION) || Objects.equals(right, TRUE_EXPRESSION)) {
            return left;
        }

        return new AndExpression(left, right);
    }

    @Override
    public Expression visitPermissionClass(ExpressionParser.PermissionClassContext ctx) {
        Check check = getCheck(dictionary, ctx.getText());
        if (check instanceof FilterExpressionCheck) {
            //FilterExpressionCheck already evaluated in datastore.
            return NOOP_EXPRESSION;
        }

        if (check instanceof UserCheck) {
            boolean userCheckResult = ((UserCheck) check).ok(requestScope.getUser());
            return userCheckResult ? TRUE_EXPRESSION : FALSE_EXPRESSION;

        }
        return expressionGenerator.apply(check);
    }

}
