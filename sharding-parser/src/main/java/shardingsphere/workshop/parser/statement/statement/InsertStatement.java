
package shardingsphere.workshop.parser.statement.statement;

import lombok.Getter;
import lombok.Setter;
import shardingsphere.workshop.parser.statement.ASTNode;
import shardingsphere.workshop.parser.statement.segment.InsertColumnsSegment;
import shardingsphere.workshop.parser.statement.segment.InsertValuesSegment;
import shardingsphere.workshop.parser.statement.segment.TableNameSegment;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Use statement.
 *
 * @author panjuan
 */
@Getter
@Setter
public final class InsertStatement implements ASTNode {

    private TableNameSegment table;
    private InsertColumnsSegment insertColumns;
    private final Collection<InsertValuesSegment> values = new LinkedList<>();
}
