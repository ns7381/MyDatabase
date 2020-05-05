
package shardingsphere.workshop.parser.engine.visitor;

import autogen.MySQLStatementBaseVisitor;
import autogen.MySQLStatementParser;
import autogen.MySQLStatementParser.IdentifierContext;
import autogen.MySQLStatementParser.SchemaNameContext;
import autogen.MySQLStatementParser.UseContext;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.TerminalNode;
import shardingsphere.workshop.parser.statement.ASTNode;
import shardingsphere.workshop.parser.statement.segment.*;
import shardingsphere.workshop.parser.statement.segment.expr.CommonExpressionSegment;
import shardingsphere.workshop.parser.statement.segment.expr.ExpressionSegment;
import shardingsphere.workshop.parser.statement.segment.expr.LiteralExpressionSegment;
import shardingsphere.workshop.parser.statement.segment.generic.DataTypeLengthSegment;
import shardingsphere.workshop.parser.statement.segment.generic.DataTypeSegment;
import shardingsphere.workshop.parser.statement.segment.generic.OwnerSegment;
import shardingsphere.workshop.parser.statement.segment.generic.SchemeNameSegment;
import shardingsphere.workshop.parser.statement.segment.predicate.AndPredicate;
import shardingsphere.workshop.parser.statement.segment.predicate.PredicateCompareRightValue;
import shardingsphere.workshop.parser.statement.segment.predicate.PredicateRightValue;
import shardingsphere.workshop.parser.statement.segment.predicate.PredicateSegment;
import shardingsphere.workshop.parser.statement.segment.projection.*;
import shardingsphere.workshop.parser.statement.segment.value.*;
import shardingsphere.workshop.parser.statement.statement.CreateTableStatement;
import shardingsphere.workshop.parser.statement.statement.InsertStatement;
import shardingsphere.workshop.parser.statement.statement.SelectStatement;
import shardingsphere.workshop.parser.statement.statement.UseStatement;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * MySQL visitor.
 */
public final class SQLVisitor extends MySQLStatementBaseVisitor<ASTNode> {

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
    public ASTNode visitColumnName(MySQLStatementParser.ColumnNameContext ctx) {
        OwnerSegment owner = null;
        if (null != ctx.owner()) {
            owner = (OwnerSegment) visit(ctx.owner());
        }
        return new ColumnSegment(owner, (IdentifierSegment) visit(ctx.name()));
    }

    @Override
    public final ASTNode visitColumnNames(final MySQLStatementParser.ColumnNamesContext ctx) {
        CollectionValue<ColumnSegment> result = new CollectionValue<>();
        for (MySQLStatementParser.ColumnNameContext each : ctx.columnName()) {
            result.getValue().add((ColumnSegment) visit(each));
        }
        return result;
    }

    @Override
    public ASTNode visitTableName(MySQLStatementParser.TableNameContext ctx) {
        OwnerSegment owner = null;
        if (null != ctx.owner()) {
            owner = (OwnerSegment) visit(ctx.owner());
        }
        return new TableNameSegment(owner, (IdentifierSegment) visit(ctx.name()));
    }

    @Override
    public ASTNode visitUse(final UseContext ctx) {
        SchemeNameSegment schemeName = (SchemeNameSegment) visit(ctx.schemaName());
        return new UseStatement(schemeName);
    }

    @Override
    public ASTNode visitSelect(MySQLStatementParser.SelectContext ctx) {
        // TODO: union select
        return visit(ctx.unionClause().selectClause(0));
    }

    @Override
    public ASTNode visitSelectClause(MySQLStatementParser.SelectClauseContext ctx) {
        SelectStatement result = new SelectStatement();
        result.setProjections((ProjectionsSegment) visit(ctx.projections()));
        if (null != ctx.fromClause()) {
            result.setTableName((TableNameSegment) visit(ctx.fromClause()));
        }
        if (null != ctx.whereClause()) {
            result.setWhere((WhereSegment) visit(ctx.whereClause()));
        }
        return result;
    }

    @Override
    public ASTNode visitProjections(MySQLStatementParser.ProjectionsContext ctx) {
        Collection<ProjectionSegment> projections = new LinkedList<>();
        if (null != ctx.unqualifiedShorthand()) {
            projections.add(new ShorthandProjectionSegment());
        }
        for (MySQLStatementParser.ProjectionContext each : ctx.projection()) {
            projections.add((ProjectionSegment) visit(each));
        }
        ProjectionsSegment result = new ProjectionsSegment();
        result.getProjections().addAll(projections);
        return result;
    }

    @Override
    public ASTNode visitProjection(MySQLStatementParser.ProjectionContext ctx) {
        if (null != ctx.columnName()) {
            ColumnSegment column = (ColumnSegment) visit(ctx.columnName());
            return new ColumnProjectionSegment(column);
        }
        if (null != ctx.expr()) {
            ASTNode projection = visit(ctx.expr());
            if (projection instanceof CommonExpressionSegment) {
                CommonExpressionSegment segment = (CommonExpressionSegment) projection;
                return new ExpressionProjectionSegment(segment.getText());
            }
        }
        // TODO complex projection
        return new ExpressionProjectionSegment("");
    }

    @Override
    public ASTNode visitFromClause(MySQLStatementParser.FromClauseContext ctx) {
        //TODO: only parse one tableName for simplify
        return (TableNameSegment) visit(ctx.tableReferences().escapedTableReference(0).tableReference().tableFactor().tableName());
    }

    @Override
    public ASTNode visitWhereClause(MySQLStatementParser.WhereClauseContext ctx) {
        WhereSegment result = new WhereSegment();
        ASTNode segment = visit(ctx.expr());
        if (segment instanceof CollectionValue) {
            AndPredicate andPredicate = new AndPredicate();
            andPredicate.setPredicates((CollectionValue<PredicateSegment>) segment);
            result.setAndPredicate(andPredicate);
        }
        return result;
    }

    @Override
    public ASTNode visitExpr(MySQLStatementParser.ExprContext ctx) {
        CollectionValue<PredicateSegment> predicates = new CollectionValue<>();
        if (null != ctx.booleanPrimary()) {
            return visit(ctx.booleanPrimary());
        }

        for (int i = 0; i < ctx.expr().size(); i++) {
            ASTNode segment = visit(ctx.expr().get(i));
            if (segment instanceof PredicateSegment) {
                predicates.getValue().add((PredicateSegment) segment);
            }
        }
        // TODO deal with XOR
        return predicates;
    }

    @Override
    public ASTNode visitBooleanPrimary(MySQLStatementParser.BooleanPrimaryContext ctx) {
        if (null != ctx.comparisonOperator() || null != ctx.SAFE_EQ_()) {
            return createCompareSegment(ctx);
        }
        if (null != ctx.predicate()) {
            return visit(ctx.predicate());
        }
        // TODO sub query
        //TODO deal with IS NOT? (TRUE | FALSE | UNKNOWN | NULL)
        return new CommonExpressionSegment(ctx.getText());
    }

    @Override
    public ASTNode visitPredicate(MySQLStatementParser.PredicateContext ctx) {
        if (1 == ctx.children.size()) {
            return visit(ctx.bitExpr(0));
        }
        for (MySQLStatementParser.BitExprContext each : ctx.bitExpr()) {
            visit(each);
        }
        for (MySQLStatementParser.ExprContext each : ctx.expr()) {
            visit(each);
        }
        for (MySQLStatementParser.SimpleExprContext each : ctx.simpleExpr()) {
            visit(each);
        }
        if (null != ctx.predicate()) {
            visit(ctx.predicate());
        }
        return new CommonExpressionSegment(ctx.getText());
    }

    @Override
    public ASTNode visitBitExpr(MySQLStatementParser.BitExprContext ctx) {
        if (null != ctx.simpleExpr()) {
            return createExpressionSegment(visit(ctx.simpleExpr()), ctx);
        }
        return new CommonExpressionSegment(ctx.getText());
    }

    @Override
    public ASTNode visitSimpleExpr(MySQLStatementParser.SimpleExprContext ctx) {
        if (null != ctx.parameterMarker()) {
            return visit(ctx.parameterMarker());
        }
        if (null != ctx.literals()) {
            return visit(ctx.literals());
        }
        if (null != ctx.intervalExpression()) {
            return visit(ctx.intervalExpression());
        }
        if (null != ctx.functionCall()) {
            return visit(ctx.functionCall());
        }
        if (null != ctx.columnName()) {
            return visit(ctx.columnName());
        }
        return visitRemainSimpleExpr(ctx);
    }

    @Override
    public ASTNode visitCreateTable(MySQLStatementParser.CreateTableContext ctx) {
        CreateTableStatement result = new CreateTableStatement((TableNameSegment) visit(ctx.tableName()));
        if (null != ctx.createDefinitionClause()) {
            CollectionValue<CreateDefinitionSegment> createDefinitions = (CollectionValue<CreateDefinitionSegment>) visit(ctx.createDefinitionClause());
            for (CreateDefinitionSegment each : createDefinitions.getValue()) {
                if (each instanceof ColumnDefinitionSegment) {
                    result.getColumnDefinitions().add((ColumnDefinitionSegment) each);
                } else if (each instanceof ConstraintDefinitionSegment) {
                    result.getConstraintDefinitions().add((ConstraintDefinitionSegment) each);
                }
            }
        }
        return result;
    }

    @Override
    public ASTNode visitCreateDefinitionClause(MySQLStatementParser.CreateDefinitionClauseContext ctx) {
        CollectionValue<CreateDefinitionSegment> result = new CollectionValue<>();
        for (MySQLStatementParser.CreateDefinitionContext each : ctx.createDefinition()) {
            if (null != each.columnDefinition()) {
                result.getValue().add((ColumnDefinitionSegment) visit(each.columnDefinition()));
            }
            if (null != each.constraintDefinition()) {
                result.getValue().add((ConstraintDefinitionSegment) visit(each.constraintDefinition()));
            }
            if (null != each.checkConstraintDefinition()) {
                result.getValue().add((ConstraintDefinitionSegment) visit(each.checkConstraintDefinition()));
            }
        }
        return result;
    }

    @Override
    public ASTNode visitColumnDefinition(MySQLStatementParser.ColumnDefinitionContext ctx) {
        ColumnSegment column = (ColumnSegment) visit(ctx.columnName());
        DataTypeSegment dataTypeSegment = (DataTypeSegment) visit(ctx.dataType());
        boolean isPrimaryKey = isPrimaryKey(ctx);
        return new ColumnDefinitionSegment(column, dataTypeSegment, isPrimaryKey);
    }

    @Override
    public ASTNode visitConstraintDefinition(MySQLStatementParser.ConstraintDefinitionContext ctx) {
        ConstraintDefinitionSegment result = new ConstraintDefinitionSegment();
        if (null != ctx.primaryKeyOption()) {
            result.getPrimaryKeyColumns().addAll(((CollectionValue<ColumnSegment>) visit(ctx.primaryKeyOption().columnNames())).getValue());
        }
        return result;
    }

    @Override
    public ASTNode visitDataType(MySQLStatementParser.DataTypeContext ctx) {
        DataTypeSegment dataTypeSegment = new DataTypeSegment();
        dataTypeSegment.setDataTypeName(((KeywordValue) visit(ctx.dataTypeName())).getValue());
        if (null != ctx.dataTypeLength()) {
            DataTypeLengthSegment dataTypeLengthSegment = (DataTypeLengthSegment) visit(ctx.dataTypeLength());
            dataTypeSegment.setDataLength(dataTypeLengthSegment);
        }
        return dataTypeSegment;
    }

    @Override
    public ASTNode visitDataTypeName(MySQLStatementParser.DataTypeNameContext ctx) {
        return new KeywordValue(ctx.getText());
    }

    @Override
    public ASTNode visitDataTypeLength(MySQLStatementParser.DataTypeLengthContext ctx) {
        DataTypeLengthSegment dataTypeLengthSegment = new DataTypeLengthSegment();
        List<TerminalNode> numbers = ctx.NUMBER_();
        if (numbers.size() == 1) {
            dataTypeLengthSegment.setPrecision(Integer.parseInt(numbers.get(0).getText()));
        }
        if (numbers.size() == 2) {
            dataTypeLengthSegment.setPrecision(Integer.parseInt(numbers.get(0).getText()));
            dataTypeLengthSegment.setScale(Integer.parseInt(numbers.get(1).getText()));
        }
        return dataTypeLengthSegment;
    }

    @Override
    public ASTNode visitLiterals(MySQLStatementParser.LiteralsContext ctx) {
        if (null != ctx.stringLiterals()) {
            return visit(ctx.stringLiterals());
        }
        if (null != ctx.numberLiterals()) {
            return visit(ctx.numberLiterals());
        }
        if (null != ctx.dateTimeLiterals()) {
            return visit(ctx.dateTimeLiterals());
        }
        if (null != ctx.hexadecimalLiterals()) {
            return visit(ctx.hexadecimalLiterals());
        }
        if (null != ctx.bitValueLiterals()) {
            return visit(ctx.bitValueLiterals());
        }
        if (null != ctx.booleanLiterals()) {
            return visit(ctx.booleanLiterals());
        }
        if (null != ctx.nullValueLiterals()) {
            return visit(ctx.nullValueLiterals());
        }
        throw new IllegalStateException("Literals must have string, number, dateTime, hex, bit, boolean or null.");
    }

    @Override
    public final ASTNode visitStringLiterals(final MySQLStatementParser.StringLiteralsContext ctx) {
        return new StringLiteralValue(ctx.getText());
    }

    @Override
    public final ASTNode visitNumberLiterals(final MySQLStatementParser.NumberLiteralsContext ctx) {
        return new NumberLiteralValue(ctx.getText());
    }

    @Override
    public final ASTNode visitDateTimeLiterals(final MySQLStatementParser.DateTimeLiteralsContext ctx) {
        // TODO deal with dateTimeLiterals
        return new OtherLiteralValue(ctx.getText());
    }

    @Override
    public final ASTNode visitHexadecimalLiterals(final MySQLStatementParser.HexadecimalLiteralsContext ctx) {
        // TODO deal with hexadecimalLiterals
        return new OtherLiteralValue(ctx.getText());
    }

    @Override
    public final ASTNode visitBitValueLiterals(final MySQLStatementParser.BitValueLiteralsContext ctx) {
        // TODO deal with bitValueLiterals
        return new OtherLiteralValue(ctx.getText());
    }

    @Override
    public final ASTNode visitBooleanLiterals(final MySQLStatementParser.BooleanLiteralsContext ctx) {
        return new BooleanLiteralValue(ctx.getText());
    }

    @Override
    public final ASTNode visitNullValueLiterals(final MySQLStatementParser.NullValueLiteralsContext ctx) {
        // TODO deal with nullValueLiterals
        return new OtherLiteralValue(ctx.getText());
    }

    @Override
    public ASTNode visitInsert(MySQLStatementParser.InsertContext ctx) {
        // TODO :FIXME, since there is no segment for insertValuesClause, InsertStatement is created by sub rule.
        InsertStatement result = new InsertStatement();
        if (null != ctx.insertValuesClause()) {
            result = (InsertStatement) visit(ctx.insertValuesClause());
        }
        result.setTable((TableNameSegment) visit(ctx.tableName()));
        return result;
    }

    @Override
    public ASTNode visitInsertValuesClause(MySQLStatementParser.InsertValuesClauseContext ctx) {
        InsertStatement result = new InsertStatement();
        if (null != ctx.columnNames()) {
            MySQLStatementParser.ColumnNamesContext columnNames = ctx.columnNames();
            CollectionValue<ColumnSegment> columnSegments = (CollectionValue<ColumnSegment>) visit(columnNames);
            result.setInsertColumns(new InsertColumnsSegment(columnSegments.getValue()));
        } else {
            result.setInsertColumns(new InsertColumnsSegment(Collections.emptyList()));
        }
        result.getValues().addAll(createInsertValuesSegments(ctx.assignmentValues()));
        return result;
    }

    @Override
    public ASTNode visitAssignmentValues(MySQLStatementParser.AssignmentValuesContext ctx) {
        List<ExpressionSegment> segments = new LinkedList<>();
        for (MySQLStatementParser.AssignmentValueContext each : ctx.assignmentValue()) {
              segments.add((ExpressionSegment) visit(each));
        }
        return new InsertValuesSegment(segments);
    }

    @Override
    public ASTNode visitAssignmentValue(MySQLStatementParser.AssignmentValueContext ctx) {
        MySQLStatementParser.ExprContext expr = ctx.expr();
        if (null != expr) {
            return visit(expr);
        }
        return new CommonExpressionSegment(ctx.getText());
    }

    private boolean isPrimaryKey(final MySQLStatementParser.ColumnDefinitionContext ctx) {
        for (MySQLStatementParser.StorageOptionContext each : ctx.storageOption()) {
            if (null != each.dataTypeGenericOption() && null != each.dataTypeGenericOption().primaryKey()) {
                return true;
            }
        }
        for (MySQLStatementParser.GeneratedOptionContext each : ctx.generatedOption()) {
            if (null != each.dataTypeGenericOption() && null != each.dataTypeGenericOption().primaryKey()) {
                return true;
            }
        }
        return false;
    }

    private ASTNode createCompareSegment(final MySQLStatementParser.BooleanPrimaryContext ctx) {
        ASTNode leftValue = visit(ctx.booleanPrimary());
        if (!(leftValue instanceof ColumnSegment)) {
            return leftValue;
        }
        PredicateRightValue rightValue = (PredicateRightValue) createPredicateRightValue(ctx);
        return new PredicateSegment((ColumnSegment) leftValue, rightValue);
    }

    private ASTNode createPredicateRightValue(final MySQLStatementParser.BooleanPrimaryContext ctx) {
        if (null != ctx.subquery()) {
            //TODO not support sub query
            return null;
        }
        ASTNode rightValue = visit(ctx.predicate());
        return createPredicateRightValue(ctx, rightValue);
    }

    private ASTNode createPredicateRightValue(final MySQLStatementParser.BooleanPrimaryContext ctx, final ASTNode rightValue) {
        if (rightValue instanceof ColumnSegment) {
            return rightValue;
        }
        return new PredicateCompareRightValue(ctx.comparisonOperator().getText(), (ExpressionSegment) rightValue);
    }

    private ASTNode createExpressionSegment(final ASTNode astNode, final ParserRuleContext context) {
        if (astNode instanceof StringLiteralValue) {
            return new LiteralExpressionSegment(((StringLiteralValue) astNode).getValue());
        }
        if (astNode instanceof NumberLiteralValue) {
            return new LiteralExpressionSegment(((NumberLiteralValue) astNode).getValue());
        }
        if (astNode instanceof BooleanLiteralValue) {
            return new LiteralExpressionSegment(((BooleanLiteralValue) astNode).getValue());
        }
        if (astNode instanceof OtherLiteralValue) {
            return new CommonExpressionSegment(context.getText());
        }
        return astNode;
    }

    private ASTNode visitRemainSimpleExpr(final MySQLStatementParser.SimpleExprContext ctx) {
        if (null != ctx.caseExpression()) {
            return visit(ctx.caseExpression());
        }
        for (MySQLStatementParser.ExprContext each : ctx.expr()) {
            visit(each);
        }
        for (MySQLStatementParser.SimpleExprContext each : ctx.simpleExpr()) {
            visit(each);
        }
        return new CommonExpressionSegment(ctx.getText());
    }

    private Collection<InsertValuesSegment> createInsertValuesSegments(final Collection<MySQLStatementParser.AssignmentValuesContext> assignmentValuesContexts) {
        Collection<InsertValuesSegment> result = new LinkedList<>();
        for (MySQLStatementParser.AssignmentValuesContext each : assignmentValuesContexts) {
            result.add((InsertValuesSegment) visit(each));
        }
        return result;
    }
}
