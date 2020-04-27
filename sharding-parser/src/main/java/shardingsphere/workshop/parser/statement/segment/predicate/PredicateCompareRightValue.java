
package shardingsphere.workshop.parser.statement.segment.predicate;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import shardingsphere.workshop.parser.statement.segment.expr.ExpressionSegment;

/**
 * Predicate right value for compare operator.
 */
@RequiredArgsConstructor
@Getter
public final class PredicateCompareRightValue implements PredicateRightValue {

    private final String operator;

    private final ExpressionSegment expression;
}
