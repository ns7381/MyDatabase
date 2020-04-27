
package shardingsphere.workshop.parser.statement.segment;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import shardingsphere.workshop.parser.statement.ASTNode;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Constraint definition segment.
 */
@RequiredArgsConstructor
@Getter
@Setter
public final class ConstraintDefinitionSegment implements CreateDefinitionSegment {

    private final Collection<ColumnSegment> primaryKeyColumns = new LinkedList<>();

}
