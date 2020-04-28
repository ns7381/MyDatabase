
package shardingsphere.workshop.parser.statement.statement;

import lombok.Getter;
import lombok.Setter;
import shardingsphere.workshop.parser.statement.ASTNode;
import shardingsphere.workshop.parser.statement.segment.*;
import shardingsphere.workshop.parser.statement.segment.projection.ProjectionsSegment;

/**
 * Use statement.
 *
 * @author panjuan
 */
@Getter
@Setter
public final class SelectStatement implements SQLStatement {

    private TableNameSegment tableName;
    private ProjectionsSegment projections;
    private WhereSegment where;
}
