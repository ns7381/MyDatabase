
package shardingsphere.workshop.parser.statement.segment;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import shardingsphere.workshop.parser.statement.ASTNode;

import java.util.Collection;

/**
 * Insert columns segment.
 */
@RequiredArgsConstructor
@Getter
public final class InsertColumnsSegment implements ASTNode {

    private final Collection<ColumnSegment> columns;
}
