
grammar MySQLStatement;

import Symbol, Comments, DMLStatement, DDLStatement, DALStatement;

execute
    : (use
    | select
    | insert
    | update
    | delete
    | createTable
    | alterTable
    | truncateTable
    | dropTable
    | showTables
    | createIndex
    | dropIndex
    | createDatabase
    | showDatabases
    ) SEMI_?
    ;
