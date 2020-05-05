
package shardingsphere.workshop.parser.statement.segment.predicate;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import shardingsphere.workshop.parser.statement.ASTNode;
import shardingsphere.workshop.parser.statement.segment.predicate.PredicateSegment;
import shardingsphere.workshop.parser.statement.segment.value.CollectionValue;

import java.util.Collection;
import java.util.LinkedList;

/**
 * And predicate.
 */
@Getter
@Setter
public final class AndPredicate implements ASTNode {


    private CollectionValue<PredicateSegment> predicates;
}
