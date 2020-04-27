package shardingsphere.workshop.parser.statement.segment.projection;

import lombok.Getter;
import shardingsphere.workshop.parser.statement.segment.ColumnSegment;
import shardingsphere.workshop.parser.statement.segment.projection.ProjectionSegment;

public class ColumnProjectionSegment implements ProjectionSegment {
    @Getter
    private final ColumnSegment column;
    public ColumnProjectionSegment(final ColumnSegment columnSegment) {
        column = columnSegment;
    }
}
