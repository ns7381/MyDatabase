
package shardingsphere.workshop.parser.engine.visitor;

import autogen.MySqlParser;
import autogen.MySqlParserBaseVisitor;
import shardingsphere.workshop.parser.statement.ASTNode;

/**
 * MySQL visitor.
 */
public final class SQLVisitor extends MySqlParserBaseVisitor<ASTNode> {

    /**
     * {@inheritDoc}
     *
     * <p>The default implementation returns the result of calling
     * {@link #visitChildren} on {@code ctx}.</p>
     *
     * @param ctx
     */
    @Override
    public ASTNode visitSqlStatements(MySqlParser.SqlStatementsContext ctx) {
        return super.visitSqlStatements(ctx);
    }
}
