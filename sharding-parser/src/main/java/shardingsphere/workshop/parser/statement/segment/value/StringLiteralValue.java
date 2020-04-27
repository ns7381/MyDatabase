
package shardingsphere.workshop.parser.statement.segment.value;

import lombok.Getter;

/**
 * String literal value.
 */
@Getter
public final class StringLiteralValue implements LiteralValue<String> {

    private final String value;

    public StringLiteralValue(final String value) {
        this.value = value.substring(1, value.length() - 1);
    }
}
