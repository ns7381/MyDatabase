
package shardingsphere.workshop.parser.statement.segment.value;

import lombok.Getter;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Collection value.
 */
@Getter
public final class CollectionValue<T> implements ValueASTNode<Collection> {

    private final Collection<T> value = new LinkedList<>();

    /**
     * Put all values from another collection value into this one.
     *
     * @param collectionValue collection value
     */
    public void combine(final CollectionValue<T> collectionValue) {
        value.addAll(collectionValue.getValue());
    }
}
