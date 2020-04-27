
package shardingsphere.workshop.parser.statement.segment.predicate;

import lombok.Getter;
import shardingsphere.workshop.parser.statement.ASTNode;
import shardingsphere.workshop.parser.statement.segment.predicate.PredicateSegment;

import java.util.Collection;
import java.util.LinkedList;

/**
 * And predicate.
 */
@Getter
public final class AndPredicate implements ASTNode {


    private final Collection<PredicateSegment> predicates = new LinkedList<>();
}
