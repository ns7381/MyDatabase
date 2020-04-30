package com.my.database.execute.engine;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.my.database.lsm.table.Row;
import com.my.database.lsm.table.Table;
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
import shardingsphere.workshop.parser.statement.segment.predicate.PredicateCompareRightValue;
import shardingsphere.workshop.parser.statement.segment.predicate.PredicateRightValue;
import shardingsphere.workshop.parser.statement.segment.projection.ColumnProjectionSegment;
import shardingsphere.workshop.parser.statement.segment.projection.ProjectionSegment;
import shardingsphere.workshop.parser.statement.statement.SQLStatement;
import shardingsphere.workshop.parser.statement.statement.SelectStatement;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class ExecuteEngine {

    private static final int MAX_CONNECTION = 1000;


    public static void execute(final ChannelHandlerContext context, final String sql) throws InterruptedException {
        // 1. parse sql to statement
        SQLStatement sqlStatement = (SQLStatement) ParseEngine.parse(sql);

        // 2. async execute task
        /*ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("sql-execute-engine-%d").build();
        new ThreadPoolExecutor(0, MAX_CONNECTION,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), threadFactory).execute(new ExecutorTask(context, sqlStatement));*/
        if (sqlStatement instanceof SelectStatement) {
            SelectStatement selectStatement = (SelectStatement) sqlStatement;
            Collection<ProjectionSegment> projections = selectStatement.getProjections().getProjections();

            List<String> columns = projections.stream()
                    .filter(pro -> pro instanceof ColumnProjectionSegment)
                    .map(pro -> ((ColumnProjectionSegment) pro).getColumn().getName().getValue())
                    .collect(Collectors.toList());
            String[] columnNames = new String[columns.size()];
            columns.toArray(columnNames);
            String tableName = selectStatement.getTableName().getName().getValue();
            Table table = new Table(tableName, columnNames);

            selectStatement.getWhere().getAndPredicates().forEach(predicate -> {
                predicate.getPredicates().forEach(pre-> {
                    String columnName = pre.getColumn().getName().getValue();
                    PredicateCompareRightValue rightValue = (PredicateCompareRightValue) pre.getRightValue();

                });
            });
            Row row100 = table.get("row100");
            context.write(new MySQLFieldCountPacket(1, 1));
            context.write(new MySQLColumnDefinition41Packet(2, 0, "sharding_db", tableName, tableName, "c1", "c1", 100, MySQLColumnType.MYSQL_TYPE_STRING,0));
            context.write(new MySQLEofPacket(3));
            context.write(new MySQLTextResultSetRowPacket(4, ImmutableList.of(row100.getCols().get("c1"))));
            context.write(new MySQLEofPacket(5));
        }
    }
}
