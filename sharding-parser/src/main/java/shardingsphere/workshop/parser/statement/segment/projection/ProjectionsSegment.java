
package shardingsphere.workshop.parser.statement.segment.projection;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import shardingsphere.workshop.parser.statement.ASTNode;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Scheme name segment.
 *
 * @author panjuan
 */
@RequiredArgsConstructor
@Getter
public final class ProjectionsSegment implements ASTNode {

    private final Collection<ProjectionSegment> projections = new LinkedList<>();
}
