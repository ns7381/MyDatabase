
package shardingsphere.workshop.parser.statement.segment.value;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Keyword value.
 */
@RequiredArgsConstructor
@Getter
public final class KeywordValue implements ValueASTNode<String> {

    private final String value;
}
