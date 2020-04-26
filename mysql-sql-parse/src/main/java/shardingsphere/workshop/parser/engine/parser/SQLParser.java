
package shardingsphere.workshop.parser.engine.parser;

import autogen.MySqlLexer;
import autogen.MySqlParser;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;

/**
 * SQL parser.
 *
 * @author panjuan
 */
public final class SQLParser extends MySqlParser {

    public SQLParser(final String sql) {
        super(new CommonTokenStream(new MySqlLexer(CharStreams.fromString(sql))));
    }

    /**
     * Parse.
     *
     * @return root node
     */
    public ParseTree parse() {
        return twoPhaseParse();
    }

    private ParseTree twoPhaseParse() {
        try {
            setErrorHandler(new BailErrorStrategy());
            getInterpreter().setPredictionMode(PredictionMode.SLL);
            return root();
        } catch (final ParseCancellationException ex) {
            reset();
            setErrorHandler(new DefaultErrorStrategy());
            getInterpreter().setPredictionMode(PredictionMode.LL);
            return root();
        }
    }
}
