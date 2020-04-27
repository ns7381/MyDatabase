
package shardingsphere.workshop.parser.statement.segment.generic;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import shardingsphere.workshop.parser.statement.ASTNode;
import shardingsphere.workshop.parser.statement.segment.IdentifierSegment;

/**
 * Scheme name segment.
 *
 * @author panjuan
 */
@RequiredArgsConstructor
@Getter
public final class SchemeNameSegment implements ASTNode {

    private final IdentifierSegment identifier;
}
