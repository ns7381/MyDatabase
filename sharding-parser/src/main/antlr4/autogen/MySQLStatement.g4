
grammar MySQLStatement;

import Symbol, SQLStatement;

execute
    : (use
    | insert
    | selectClause
    ) SEMI_?
    ;
