
package shardingsphere.workshop.parser.statement.segment.value;

import lombok.RequiredArgsConstructor;

/**
 * Boolean literal value.
 */
@RequiredArgsConstructor
public final class BooleanLiteralValue implements LiteralValue<Boolean> {

    private final boolean value;

    public BooleanLiteralValue(final String value) {
        this.value = Boolean.parseBoolean(value);
    }

    @Override
    public Boolean getValue() {
        return value;
    }
}
