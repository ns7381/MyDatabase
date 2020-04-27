
package shardingsphere.workshop.parser.statement.segment.predicate;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import shardingsphere.workshop.parser.statement.ASTNode;
import shardingsphere.workshop.parser.statement.segment.ColumnSegment;

/**
 * Predicate segment.
 */
@RequiredArgsConstructor
@Getter
public final class PredicateSegment implements ASTNode {

    private final ColumnSegment column;

    private final PredicateRightValue rightValue;
}
