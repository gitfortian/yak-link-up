package com.link.up.connector.mongodb.schema;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.PrimaryKey;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.api.table.type.FluxDataType;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Infers a portable Link-Up schema from sampled BSON documents.
 *
 * <p>The inference is intentionally conservative. Stable scalar values keep strong canonical
 * types; incompatible values and complex containers fall back to STRING so downstream relational
 * sinks do not need MongoDB-specific types.
 */
public final class MongoSchemaInference {

    private static final int DECIMAL_PRECISION = 34;
    private static final int DECIMAL_SCALE = 18;

    private final int maxDepth;
    private final Map<String, FieldState> fields = new LinkedHashMap<String, FieldState>();

    private int sampledDocuments;

    public MongoSchemaInference(int maxDepth) {
        if (maxDepth < 0) {
            throw new IllegalArgumentException("maxDepth must not be negative");
        }
        this.maxDepth = maxDepth;
    }

    public void observe(BsonDocument document) {
        Objects.requireNonNull(document, "document must not be null");
        sampledDocuments++;
        observeDocument(null, document, 0);
    }

    public int getSampledDocuments() {
        return sampledDocuments;
    }

    public TableSchema build() {
        if (fields.isEmpty()) {
            return syntheticEmptySchema();
        }

        TableSchema.Builder schema = TableSchema.builder();
        for (Map.Entry<String, FieldState> entry : fields.entrySet()) {
            schema.column(entry.getValue().toColumn(entry.getKey(), sampledDocuments));
        }

        if (fields.containsKey("_id")) {
            schema.primaryKey(PrimaryKey.of("_id_", Collections.singletonList("_id")));
        }
        return schema.build();
    }

    private void observeDocument(String parentPath, BsonDocument document, int depth) {
        for (Map.Entry<String, BsonValue> entry : document.entrySet()) {
            String path = parentPath == null
                    ? entry.getKey()
                    : parentPath + "." + entry.getKey();
            BsonValue value = entry.getValue();

            FieldState state = fields.get(path);
            if (state == null) {
                state = new FieldState();
                fields.put(path, state);
            }
            state.observe(value);

            if (value != null
                    && value.getBsonType() == BsonType.DOCUMENT
                    && depth < maxDepth) {
                observeDocument(path, value.asDocument(), depth + 1);
            }
        }
    }

    private static TableSchema syntheticEmptySchema() {
        Column id = Column.builder("_id", BasicType.STRING_TYPE)
                .nullable(false)
                .sourceType("ObjectId")
                .attribute("mongodb.path", "_id")
                .attribute("mongodb.synthetic", "true")
                .build();

        return TableSchema.builder()
                .column(id)
                .primaryKey(PrimaryKey.of("_id_", Collections.singletonList("_id")))
                .build();
    }

    private static final class FieldState {

        private final Set<BsonType> bsonTypes = new LinkedHashSet<BsonType>();

        private InferredKind kind = InferredKind.NULL;
        private int observedDocuments;
        private boolean sawNull;
        private boolean sawComplex;

        private void observe(BsonValue value) {
            observedDocuments++;
            BsonType bsonType = value == null ? BsonType.NULL : value.getBsonType();
            bsonTypes.add(bsonType);

            if (bsonType == BsonType.NULL) {
                sawNull = true;
            }
            if (bsonType == BsonType.DOCUMENT || bsonType == BsonType.ARRAY) {
                sawComplex = true;
            }

            kind = InferredKind.merge(kind, InferredKind.from(bsonType));
        }

        private Column toColumn(String path, int totalDocuments) {
            FluxDataType<?> dataType = kind.toDataType();
            boolean nullable = sawNull || observedDocuments < totalDocuments;

            Column.Builder builder = Column.builder(path, dataType)
                    .nullable(nullable)
                    .sourceType(sourceType())
                    .attribute("mongodb.path", path)
                    .attribute("mongodb.bsonTypes", bsonTypeList());

            if (path.indexOf('.') >= 0) {
                builder.attribute("mongodb.nested", "true");
            }
            if (sawComplex) {
                builder.attribute("mongodb.complex", "true")
                        .attribute("mongodb.encoding", "extended-json");
            }
            if (nonNullTypeCount() > 1) {
                builder.attribute("mongodb.heterogeneous", "true");
            }
            if (kind == InferredKind.DECIMAL) {
                builder.precision(DECIMAL_PRECISION).scale(DECIMAL_SCALE);
            }
            return builder.build();
        }

        private String sourceType() {
            StringBuilder result = new StringBuilder();
            for (BsonType bsonType : bsonTypes) {
                if (result.length() > 0) {
                    result.append('|');
                }
                result.append(displayName(bsonType));
            }
            return result.length() == 0 ? "Unknown" : result.toString();
        }

        private String bsonTypeList() {
            StringBuilder result = new StringBuilder();
            for (BsonType bsonType : bsonTypes) {
                if (result.length() > 0) {
                    result.append(',');
                }
                result.append(bsonType.name());
            }
            return result.toString();
        }

        private int nonNullTypeCount() {
            int count = 0;
            for (BsonType bsonType : bsonTypes) {
                if (bsonType != BsonType.NULL) {
                    count++;
                }
            }
            return count;
        }

        private static String displayName(BsonType bsonType) {
            switch (bsonType) {
                case OBJECT_ID:
                    return "ObjectId";
                case INT32:
                    return "Int32";
                case INT64:
                    return "Int64";
                case DECIMAL128:
                    return "Decimal128";
                case DATE_TIME:
                    return "DateTime";
                case TIMESTAMP:
                    return "Timestamp";
                case BINARY:
                    return "Binary";
                case DOCUMENT:
                    return "Document";
                case ARRAY:
                    return "Array";
                case STRING:
                    return "String";
                case BOOLEAN:
                    return "Boolean";
                case DOUBLE:
                    return "Double";
                case NULL:
                    return "Null";
                default:
                    return bsonType.name();
            }
        }
    }

    private enum InferredKind {
        NULL,
        BOOLEAN,
        INT,
        LONG,
        DOUBLE,
        DECIMAL,
        TIMESTAMP,
        BYTES,
        JSON,
        STRING;

        private static InferredKind from(BsonType bsonType) {
            switch (bsonType) {
                case NULL:
                    return NULL;
                case BOOLEAN:
                    return BOOLEAN;
                case INT32:
                    return INT;
                case INT64:
                    return LONG;
                case DOUBLE:
                    return DOUBLE;
                case DECIMAL128:
                    return DECIMAL;
                case DATE_TIME:
                case TIMESTAMP:
                    return TIMESTAMP;
                case BINARY:
                    return BYTES;
                case DOCUMENT:
                case ARRAY:
                    return JSON;
                default:
                    return STRING;
            }
        }

        private static InferredKind merge(InferredKind left, InferredKind right) {
            if (left == NULL) {
                return right;
            }
            if (right == NULL) {
                return left;
            }
            if (left == right) {
                return left;
            }
            if (isNumeric(left) && isNumeric(right)) {
                return mergeNumeric(left, right);
            }
            return STRING;
        }

        private static InferredKind mergeNumeric(InferredKind left, InferredKind right) {
            if ((left == DECIMAL && right == DOUBLE)
                    || (left == DOUBLE && right == DECIMAL)) {
                return STRING;
            }
            if (left == DECIMAL || right == DECIMAL) {
                return DECIMAL;
            }
            if (left == DOUBLE || right == DOUBLE) {
                return DOUBLE;
            }
            if (left == LONG || right == LONG) {
                return LONG;
            }
            return INT;
        }

        private static boolean isNumeric(InferredKind kind) {
            return kind == INT || kind == LONG || kind == DOUBLE || kind == DECIMAL;
        }

        private FluxDataType<?> toDataType() {
            switch (this) {
                case BOOLEAN:
                    return BasicType.BOOLEAN_TYPE;
                case INT:
                    return BasicType.INT_TYPE;
                case LONG:
                    return BasicType.LONG_TYPE;
                case DOUBLE:
                    return BasicType.DOUBLE_TYPE;
                case DECIMAL:
                    return new DecimalType(DECIMAL_PRECISION, DECIMAL_SCALE);
                case TIMESTAMP:
                    return BasicType.TIMESTAMP_TYPE;
                case BYTES:
                    return BasicType.BYTES_TYPE;
                case NULL:
                case JSON:
                case STRING:
                default:
                    return BasicType.STRING_TYPE;
            }
        }
    }
}
