package com.my.database.execute.engine;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.ChannelHandlerContext;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import shardingsphere.workshop.parser.engine.ParseEngine;
import shardingsphere.workshop.parser.statement.statement.SQLStatement;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public final class ExecuteEngine {

    private static final int MAX_CONNECTION = 1000;


    public static void execute(final ChannelHandlerContext context, final String sql) {
        // 1. parse sql to statement
        SQLStatement sqlStatement = (SQLStatement) ParseEngine.parse(sql);

        // 2. async execute task
        /*ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("sql-execute-engine-%d").build();
        new ThreadPoolExecutor(0, MAX_CONNECTION,
                60L, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), threadFactory).execute(new ExecutorTask(context, sqlStatement));*/
    }
}
