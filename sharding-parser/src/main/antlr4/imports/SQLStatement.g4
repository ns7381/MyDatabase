
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

select
    : SELECT columnName FROM tableName ( WHERE compareColumnName comparisonOperator value )?
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

compareColumnName
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

comparisonOperator
    : '=' | '>' | '<' | '<' '=' | '>' '='
    | '<' '>' | '!' '=' | '<' '=' '>'
    ;
    