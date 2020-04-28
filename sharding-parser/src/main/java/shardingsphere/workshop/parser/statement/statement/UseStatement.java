
package shardingsphere.workshop.parser.statement.statement;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import shardingsphere.workshop.parser.statement.ASTNode;
import shardingsphere.workshop.parser.statement.segment.generic.SchemeNameSegment;

/**
 * Use statement.
 *
 * @author panjuan
 */
@RequiredArgsConstructor
@Getter
public final class UseStatement implements SQLStatement {

    private final SchemeNameSegment schemeName;
}
