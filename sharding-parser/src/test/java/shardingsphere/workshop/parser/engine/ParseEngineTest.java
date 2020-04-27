
package shardingsphere.workshop.parser.engine;

import org.junit.Test;
import shardingsphere.workshop.parser.statement.statement.CreateTableStatement;
import shardingsphere.workshop.parser.statement.statement.InsertStatement;
import shardingsphere.workshop.parser.statement.statement.SelectStatement;
import shardingsphere.workshop.parser.statement.statement.UseStatement;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public final class ParseEngineTest {

    @Test
    public void testParse() {
        String sql = "use sharding_db";
        UseStatement useStatement = (UseStatement) ParseEngine.parse(sql);
        assertThat(useStatement.getSchemeName().getIdentifier().getValue(), is("sharding_db"));
    }

    @Test
    public void testParseCreateTable() {
        String sql = "CREATE TABLE IF NOT EXISTS t_order(\n" +
                "   id INT UNSIGNED AUTO_INCREMENT,\n" +
                "   name VARCHAR(100) NOT NULL,\n" +
                "   creator VARCHAR(40) NOT NULL,\n" +
                "   create_date DATE,\n" +
                "   PRIMARY KEY ( id )\n" +
                ");";
        CreateTableStatement statement = (CreateTableStatement) ParseEngine.parse(sql);
        assertThat(statement.getTable().getName().getValue(), is("t_order"));
    }

    @Test
    public void testParseInsert() {
        String sql = "INSERT INTO t_order ( id, name, creator )\n" +
                "                       VALUES\n" +
                "                       ( 1, 'n1', 'c1' );";
        InsertStatement statement = (InsertStatement) ParseEngine.parse(sql);
        assertThat(statement.getTable().getName().getValue(), is("t_order"));
    }

    @Test
    public void testParseSelect() {
        String sql = "select * from t_order where id > 10";
        SelectStatement statement = (SelectStatement) ParseEngine.parse(sql);
        assertThat(statement.getTableName().getName().getValue(), is("t_order"));
    }
}
