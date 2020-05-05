package com.my.database.execute.engine;

import com.my.database.api.Cell;
import com.my.database.api.Row;
import com.my.database.api.RowSet;
import com.my.database.api.StorageEngine;
import com.my.database.api.operator.FilterOperator;
import com.my.database.api.operator.ProjectionOperator;
import com.my.database.api.operator.UnaryOperator;
import com.my.database.bplus.BplusStorageEngine;
import com.my.database.mysql.protocol.MySQLOKPacket;
import com.my.database.mysql.protocol.constant.MySQLColumnType;
import com.my.database.mysql.protocol.query.MySQLColumnDefinition41Packet;
import com.my.database.mysql.protocol.query.MySQLEofPacket;
import com.my.database.mysql.protocol.query.MySQLFieldCountPacket;
import com.my.database.mysql.protocol.query.MySQLTextResultSetRowPacket;
import io.netty.channel.ChannelHandlerContext;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import shardingsphere.workshop.parser.engine.ParseEngine;
import shardingsphere.workshop.parser.statement.segment.ColumnSegment;
import shardingsphere.workshop.parser.statement.segment.InsertValuesSegment;
import shardingsphere.workshop.parser.statement.segment.expr.ExpressionSegment;
import shardingsphere.workshop.parser.statement.segment.expr.LiteralExpressionSegment;
import shardingsphere.workshop.parser.statement.segment.predicate.PredicateCompareRightValue;
import shardingsphere.workshop.parser.statement.segment.projection.ColumnProjectionSegment;
import shardingsphere.workshop.parser.statement.segment.projection.ExpressionProjectionSegment;
import shardingsphere.workshop.parser.statement.segment.projection.ProjectionSegment;
import shardingsphere.workshop.parser.statement.segment.projection.ShorthandProjectionSegment;
import shardingsphere.workshop.parser.statement.statement.CreateTableStatement;
import shardingsphere.workshop.parser.statement.statement.InsertStatement;
import shardingsphere.workshop.parser.statement.statement.SQLStatement;
import shardingsphere.workshop.parser.statement.statement.SelectStatement;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class ExecuteEngine {

    private static final int MAX_CONNECTION = 1000;


    public static void execute(final ChannelHandlerContext context, final String sql) throws Exception {
        // 1. parse sql to statement
        SQLStatement sqlStatement = (SQLStatement) ParseEngine.parse(sql);

        // 2. execute task
        StorageEngine storageEngine = BplusStorageEngine.getBplusStorageEngine();
        if (sqlStatement instanceof SelectStatement) {
            SelectStatement selectStatement = (SelectStatement) sqlStatement;
            executeSelectStatement(context, storageEngine, selectStatement);
        } else if (sqlStatement instanceof CreateTableStatement) {
            CreateTableStatement createTableStatement = (CreateTableStatement) sqlStatement;
            executeCreateTableStatement(context, storageEngine, createTableStatement);
        } else if (sqlStatement instanceof InsertStatement) {
            InsertStatement insertStatement = (InsertStatement) sqlStatement;
            executeInsertStatement(context, storageEngine, insertStatement);
        }
    }

    private static void executeInsertStatement(ChannelHandlerContext context, StorageEngine storageEngine, InsertStatement insertStatement) throws Exception {
        String tableName = insertStatement.getTable().getName().getValue();
        Map<String, Object> inputs = new HashMap<>();

        List<ColumnSegment> columns = (List<ColumnSegment>) insertStatement.getInsertColumns().getColumns();
        List<InsertValuesSegment> values = (List<InsertValuesSegment>) insertStatement.getValues();
        for (InsertValuesSegment insertValue : values) {
            List<ExpressionSegment> cols = insertValue.getValues();
            for (int i = 0; i < cols.size(); i++) {
                inputs.put(columns.get(i).getName().getValue(), ((LiteralExpressionSegment) cols.get(i)).getLiterals());
            }
            storageEngine.insert(tableName, inputs);
        }

        context.writeAndFlush(new MySQLOKPacket(1));
    }

    private static void executeCreateTableStatement(ChannelHandlerContext context, StorageEngine storageEngine, CreateTableStatement createTableStatement) throws Exception {
        String tableName = createTableStatement.getTable().getName().getValue();
        Row row = new Row();
        String keyColumnName = createTableStatement.getConstraintDefinitions().stream().map(constraintDefinition -> constraintDefinition.getPrimaryKeyColumns().stream().findFirst().get().getName().getValue()).findFirst().get();
        createTableStatement.getColumnDefinitions().forEach(columnDefinition -> {
            Cell cell = new Cell(columnDefinition.getDataType().getDataTypeName(), columnDefinition.getColumnName().getName().getValue());
            if (keyColumnName.equals(columnDefinition.getColumnName().getName().getValue())) {
                cell.setPrimary(true);
            }
            row.getCells().add(cell);
        });


        storageEngine.createTable(tableName, row);
        context.writeAndFlush(new MySQLOKPacket(1));
    }

    private static void executeSelectStatement(ChannelHandlerContext context, StorageEngine storageEngine, SelectStatement selectStatement) throws Exception {
        Collection<ProjectionSegment> projections = selectStatement.getProjections().getProjections();

        if (null == selectStatement.getTableName()) {
            AtomicBoolean isVersion = new AtomicBoolean(false);
            selectStatement.getProjections().getProjections().stream().findFirst().ifPresent(projectionSegment -> {
                if (projectionSegment instanceof ExpressionProjectionSegment) {
                    ExpressionProjectionSegment expressionProjectionSegment = (ExpressionProjectionSegment) projectionSegment;
                    if ("@@version_comment".equals(expressionProjectionSegment.getText())) {
                        isVersion.set(true);
                    }
                }
            });
            if (isVersion.get()) {
                context.writeAndFlush(new MySQLOKPacket(1));
                return;
            }
        }
        String tableName = selectStatement.getTableName().getName().getValue();
        List<UnaryOperator> operators = new ArrayList<>();
        boolean isAll = projections.stream().anyMatch(pro -> pro instanceof ShorthandProjectionSegment);
        if (!isAll) {
            List<String> columns = projections.stream()
                    .filter(pro -> pro instanceof ColumnProjectionSegment)
                    .map(pro -> ((ColumnProjectionSegment) pro).getColumn().getName().getValue())
                    .collect(Collectors.toList());
            if (columns != null && !columns.isEmpty()) {
                ProjectionOperator projectionOperator = new ProjectionOperator(row -> {
                    List<Cell> newCells = row.getCells().stream().filter(cell -> columns.contains(cell.getName())).collect(Collectors.toList());
                    return new Row(newCells);
                });
                operators.add(projectionOperator);
            }
        }

        selectStatement.getWhere().getAndPredicates().forEach(predicate -> {
            predicate.getPredicates().forEach(pre -> {
                String columnName = pre.getColumn().getName().getValue();
                PredicateCompareRightValue rightValue = (PredicateCompareRightValue) pre.getRightValue();
                FilterOperator filterOperator = new FilterOperator(row -> {
                    AtomicBoolean isFilter = new AtomicBoolean(false);
                    row.getCells().stream().filter(cell -> columnName.equals(cell.getName())).forEach(cell -> {
                        LiteralExpressionSegment expression = (LiteralExpressionSegment) rightValue.getExpression();
                        Object compareVal = expression.getLiterals();
                        switch (rightValue.getOperator()) {
                            case "=":
                                isFilter.set(cell.getVal().equals(compareVal));
                                break;
                            case ">=":
                                isFilter.set((Integer) cell.getVal() >= (Integer) compareVal);
                                break;
                            case ">":
                                isFilter.set((Integer) cell.getVal() > (Integer) compareVal);
                                break;
                            case "<":
                                isFilter.set((Integer) cell.getVal() < (Integer) compareVal);
                                break;
                            case "<=":
                                isFilter.set((Integer) cell.getVal() <= (Integer) compareVal);
                                break;
                        }
                    });
                    return isFilter.get();
                });
                operators.add(filterOperator);
            });
        });


        RowSet rowSet = storageEngine.select(tableName, operators);
        AtomicInteger sequenceNo = new AtomicInteger(1);
        rowSet.getRows().stream().findAny().ifPresent(row -> {
            context.write(new MySQLFieldCountPacket(sequenceNo.getAndIncrement(), row.getCells().size()));
            for (Cell cell : row.getCells()) {
                context.write(new MySQLColumnDefinition41Packet(sequenceNo.getAndIncrement(), 0, "db", tableName, tableName, cell.getName(), cell.getName(), 100, MySQLColumnType.MYSQL_TYPE_STRING, 0));
            }
        });

        context.write(new MySQLEofPacket(sequenceNo.getAndIncrement()));
        rowSet.getRows().forEach(row -> {
            context.write(new MySQLTextResultSetRowPacket(sequenceNo.getAndIncrement(), row.getCells().stream().map(Cell::getVal).collect(Collectors.toList())));
        });
        context.write(new MySQLEofPacket(sequenceNo.getAndIncrement()));
        context.flush();
    }
}
