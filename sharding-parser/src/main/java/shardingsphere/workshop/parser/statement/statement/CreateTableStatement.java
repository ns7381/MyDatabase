
package shardingsphere.workshop.parser.statement.statement;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import shardingsphere.workshop.parser.statement.ASTNode;
import shardingsphere.workshop.parser.statement.segment.ColumnDefinitionSegment;
import shardingsphere.workshop.parser.statement.segment.ConstraintDefinitionSegment;
import shardingsphere.workshop.parser.statement.segment.TableNameSegment;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Create table statement.
 */
@RequiredArgsConstructor
@Getter
public final class CreateTableStatement implements ASTNode {

    private final TableNameSegment table;

    private final Collection<ColumnDefinitionSegment> columnDefinitions = new LinkedList<>();

    private final Collection<ConstraintDefinitionSegment> constraintDefinitions = new LinkedList<>();

}
