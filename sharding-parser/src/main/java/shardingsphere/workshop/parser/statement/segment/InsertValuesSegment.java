
package shardingsphere.workshop.parser.statement.segment;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import shardingsphere.workshop.parser.statement.ASTNode;
import shardingsphere.workshop.parser.statement.segment.expr.ExpressionSegment;

import java.util.List;

/**
 * Insert values segment.
 */
@RequiredArgsConstructor
@Getter
public final class InsertValuesSegment implements ASTNode {

    private final List<ExpressionSegment> values;
}
