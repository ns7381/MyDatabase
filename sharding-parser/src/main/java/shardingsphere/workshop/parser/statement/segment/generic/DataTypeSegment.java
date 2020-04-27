
package shardingsphere.workshop.parser.statement.segment.generic;

import lombok.Getter;
import lombok.Setter;
import shardingsphere.workshop.parser.statement.ASTNode;

@Getter
@Setter
public final class DataTypeSegment implements ASTNode {

    private String dataTypeName;

    private DataTypeLengthSegment dataLength;
}
