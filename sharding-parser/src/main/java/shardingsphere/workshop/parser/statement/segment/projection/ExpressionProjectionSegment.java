
package shardingsphere.workshop.parser.statement.segment.projection;

import lombok.Getter;
import shardingsphere.workshop.parser.statement.ASTNode;
import shardingsphere.workshop.parser.statement.segment.projection.ProjectionSegment;
import shardingsphere.workshop.parser.util.SQLUtil;

/**
 * Expression projection segment.
 */
@Getter
public final class ExpressionProjectionSegment implements ProjectionSegment, ASTNode {


    private final String text;


    public ExpressionProjectionSegment(final String text) {
        this.text = SQLUtil.getExpressionWithoutOutsideParentheses(text);
    }
}
