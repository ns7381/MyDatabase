
package shardingsphere.workshop.parser.statement.segment;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import shardingsphere.workshop.parser.statement.ASTNode;
import shardingsphere.workshop.parser.statement.segment.generic.OwnerSegment;

/**
 * Scheme name segment.
 *
 * @author panjuan
 */
@RequiredArgsConstructor
@Getter
@Setter
@ToString
public final class ColumnSegment implements ASTNode {

    private final OwnerSegment owner;
    private final IdentifierSegment name;
}
