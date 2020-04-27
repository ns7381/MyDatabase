
package shardingsphere.workshop.parser.statement.segment;

import lombok.Getter;
import lombok.Setter;
import shardingsphere.workshop.parser.statement.segment.generic.DataTypeSegment;

/**
 * Column definition segment.
 */
@Getter
@Setter
public final class ColumnDefinitionSegment implements CreateDefinitionSegment {

    private ColumnSegment columnName;

    private DataTypeSegment dataType;

    private boolean primaryKey;

    public ColumnDefinitionSegment(final ColumnSegment columnName, final DataTypeSegment dataType, final boolean primaryKey) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.primaryKey = primaryKey;
    }
}
