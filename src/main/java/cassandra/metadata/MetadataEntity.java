package cassandra.metadata;

import com.fasterxml.jackson.databind.JsonNode;

abstract class MetadataEntity {

    public JsonNode toJsonNode() {
        return MetadataService.valueToJsonNode(this);
    }

    public String toJsonString() {
        return MetadataService.valueToJsonString(this);
    }

    static abstract class Builder<T extends MetadataEntity> {

        public abstract Builder<T> mergeFrom(T entity);
    }
}
