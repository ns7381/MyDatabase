
package shardingsphere.workshop.parser.statement.segment;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import shardingsphere.workshop.parser.statement.ASTNode;
import shardingsphere.workshop.parser.statement.segment.predicate.AndPredicate;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Where segment.
 */
@RequiredArgsConstructor
@Getter
@Setter
public final class WhereSegment implements ASTNode {

    private final Collection<AndPredicate> andPredicates = new LinkedList<>();
}
