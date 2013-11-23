package cassandra.metadata;

import cassandra.cql.type.CQL3Type;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ComparatorOrValidator {

    private final String className;
    private final List<CQL3Type> types;
    private final Map<String, CQL3Type> collections;

    public ComparatorOrValidator(String className) {
        this.className = className;
        CQL3Type.TypeParser parser = CQL3Type.TypeParser.parse(className);
        types = Collections.unmodifiableList(parser.types());
        collections = Collections.unmodifiableMap(parser.collections());
    }

    public String className() {
        return className;
    }

    public CQL3Type type() {
        return types.get(0);
    }

    public List<CQL3Type> types() {
        return types;
    }

    public Map<String, CQL3Type> collections() {
        return collections;
    }

    public boolean isComposite() {
        return !collections.isEmpty();
    }
}
