
package shardingsphere.workshop.parser.statement.segment.generic;

import lombok.Getter;
import lombok.Setter;
import shardingsphere.workshop.parser.statement.ASTNode;

import java.util.Optional;

@Getter
@Setter
public final class DataTypeLengthSegment implements ASTNode {

    private int precision;

    private int scale;

    /**
     * get secondNumber.
     *
     * @return Optional.
     */
    public Optional<Integer> getScale() {
        return Optional.ofNullable(scale);
    }
}
