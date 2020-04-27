
package shardingsphere.workshop.parser.statement.segment.value;


import shardingsphere.workshop.parser.statement.ASTNode;

/**
 * Value AST node.
 *
 * @param <T> type of value
 */
public interface ValueASTNode<T> extends ASTNode {

    /**
     * Get value.
     *
     * @return value
     */
    T getValue();
}
