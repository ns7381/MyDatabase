
package shardingsphere.workshop.parser.statement.segment.expr;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * Literal expression segment.
 */
@RequiredArgsConstructor
@Getter
@ToString
public class LiteralExpressionSegment implements SimpleExpressionSegment {

    private final Object literals;
}
