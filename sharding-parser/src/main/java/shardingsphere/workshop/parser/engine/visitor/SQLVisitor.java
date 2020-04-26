
package shardingsphere.workshop.parser.engine.visitor;

import autogen.MySQLStatementBaseVisitor;
import autogen.MySQLStatementParser;
import autogen.MySQLStatementParser.IdentifierContext;
import autogen.MySQLStatementParser.SchemaNameContext;
import autogen.MySQLStatementParser.UseContext;
import shardingsphere.workshop.parser.statement.ASTNode;
import shardingsphere.workshop.parser.statement.segment.*;
import shardingsphere.workshop.parser.statement.statement.SelectStatement;
import shardingsphere.workshop.parser.statement.statement.UseStatement;

/**
 * MySQL visitor.
 */
public final class SQLVisitor extends MySQLStatementBaseVisitor<ASTNode> {

    @Override
    public ASTNode visitUse(final UseContext ctx) {
        SchemeNameSegment schemeName = (SchemeNameSegment) visit(ctx.schemaName());
        return new UseStatement(schemeName);
    }

    @Override
    public ASTNode visitSchemaName(final SchemaNameContext ctx) {
        IdentifierSegment identifier = (IdentifierSegment) visit(ctx.identifier());
        return new SchemeNameSegment(identifier);
    }

    @Override
    public ASTNode visitIdentifier(final IdentifierContext ctx) {
        return new IdentifierSegment(ctx.getText());
    }

    @Override
    public ASTNode visitSelect(MySQLStatementParser.SelectContext ctx) {
        TableNameSegment tableName = (TableNameSegment) visit(ctx.tableName());
        ColumnNameSegment columnName = (ColumnNameSegment) visit(ctx.columnName());
        CompareColumnNameSegment compareColumnName = (CompareColumnNameSegment) visit(ctx.compareColumnName());
        ValueSegment value = (ValueSegment) visit(ctx.value());
        ComparisonOperatorSegment comparisonOperator = (ComparisonOperatorSegment) visit(ctx.value());
        return new SelectStatement(tableName, columnName, compareColumnName, comparisonOperator, value);
    }

    @Override
    public ASTNode visitColumnName(MySQLStatementParser.ColumnNameContext ctx) {
        IdentifierSegment identifier = (IdentifierSegment) visit(ctx.identifier());
        return new ColumnNameSegment(identifier);
    }

    @Override
    public ASTNode visitTableName(MySQLStatementParser.TableNameContext ctx) {
        IdentifierSegment identifier = (IdentifierSegment) visit(ctx.identifier());
        return new TableNameSegment(identifier);
    }

    @Override
    public ASTNode visitCompareColumnName(MySQLStatementParser.CompareColumnNameContext ctx) {
        IdentifierSegment identifier = (IdentifierSegment) visit(ctx.identifier());
        return new CompareColumnNameSegment(identifier);
    }

    @Override
    public ASTNode visitComparisonOperator(MySQLStatementParser.ComparisonOperatorContext ctx) {
        return new ComparisonOperatorSegment(ctx.getText());
    }

    @Override
    public ASTNode visitValue(MySQLStatementParser.ValueContext ctx) {
        IdentifierSegment identifier = (IdentifierSegment) visit(ctx.identifier());
        return new CompareColumnNameSegment(identifier);
    }
}
