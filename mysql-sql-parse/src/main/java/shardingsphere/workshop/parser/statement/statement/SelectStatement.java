
package shardingsphere.workshop.parser.statement.statement;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import shardingsphere.workshop.parser.statement.ASTNode;
import shardingsphere.workshop.parser.statement.segment.*;

/**
 * Use statement.
 *
 * @author panjuan
 */
@RequiredArgsConstructor
@Getter
public final class SelectStatement implements ASTNode {

    private final TableNameSegment tableName;
    private final ColumnNameSegment columnName;
    private final CompareColumnNameSegment compareColumnName;
    private final ComparisonOperatorSegment comparisonOperator;
    private final ValueSegment value;
}
