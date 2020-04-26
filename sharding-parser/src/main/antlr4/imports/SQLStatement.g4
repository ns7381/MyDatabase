
grammar SQLStatement;

import Symbol, Keyword, Literals;

use
    : USE schemaName
    ;

schemaName
    : identifier
    ;

insert
    : INSERT INTO? tableName columnNames? VALUE assignmentValues
    ;

assignmentValues
    : LP_ assignmentValue (COMMA_ assignmentValue)* RP_
    ;

assignmentValue
    : identifier
    ;

columnNames
    : LP_ columnName (COMMA_ columnName)* RP_
    ;

columnName
    : identifier
    ;

value
    : identifier
    ;

tableName
    : identifier
    ;

identifier
    : IDENTIFIER_ | STRING_ | NUMBER_
    ;

selectClause
    : SELECT selectSpecification* fromClause? whereClause?
    ;

selectSpecification
    : duplicateSpecification | HIGH_PRIORITY | STRAIGHT_JOIN | SQL_SMALL_RESULT | SQL_BIG_RESULT | SQL_BUFFER_RESULT | (SQL_CACHE | SQL_NO_CACHE) | SQL_CALC_FOUND_ROWS
    ;

duplicateSpecification
    : ALL | DISTINCT | DISTINCTROW
    ;
fromClause
    : FROM tableName
    ;
whereClause
    : WHERE expr
    ;
expr
    : expr logicalOperator expr
    | expr XOR expr
    | notOperator_ expr
    | LP_ expr RP_
    ;
logicalOperator
    : OR | OR_ | AND | AND_
    ;
notOperator_
    : NOT | NOT_
    ;

comparisonOperator
    : EQ_ | GTE_ | GT_ | LTE_ | LT_ | NEQ_
    ;
intervalExpression
    : INTERVAL expr intervalUnit_
    ;

intervalUnit_
    : MICROSECOND | SECOND | MINUTE | HOUR | DAY | WEEK | MONTH
    | QUARTER | YEAR | SECOND_MICROSECOND | MINUTE_MICROSECOND | MINUTE_SECOND | HOUR_MICROSECOND | HOUR_SECOND
    | HOUR_MINUTE | DAY_MICROSECOND | DAY_SECOND | DAY_MINUTE | DAY_HOUR | YEAR_MONTH
    ;
subquery
    : 'Default does not match anything'
    ;
