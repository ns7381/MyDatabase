
package shardingsphere.workshop.parser.statement.segment.value;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Other literal value.
 */
@RequiredArgsConstructor
@Getter
public final class OtherLiteralValue implements LiteralValue<String> {

    private final String value;
}
