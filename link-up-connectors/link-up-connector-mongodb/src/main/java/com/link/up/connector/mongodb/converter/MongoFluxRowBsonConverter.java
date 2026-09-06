package com.link.up.connector.mongodb.converter;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.FluxRow;
import com.link.up.api.table.type.SqlType;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Converts one prepared FluxRow into a MongoDB BSON document. */
public final class MongoFluxRowBsonConverter {

    private final List<FieldConversion> fields;
    private final FieldConversion documentIdField;

    public MongoFluxRowBsonConverter(
            TableSchema schema,
            String documentIdFieldName) {
        Objects.requireNonNull(schema, "schema must not be null");

        List<FieldConversion> prepared = new ArrayList<FieldConversion>(schema.getColumnCount());
        FieldConversion idField = null;
        for (int i = 0; i < schema.getColumnCount(); i++) {
            Column column = schema.getColumn(i);
            FieldConversion field = new FieldConversion(column, i);
            prepared.add(field);
            if (documentIdFieldName != null
                    && documentIdFieldName.equals(column.getName())) {
                idField = field;
            }
        }

        Collections.sort(
                prepared,
                new Comparator<FieldConversion>() {
                    @Override
                    public int compare(FieldConversion left, FieldConversion right) {
                        int depth = Integer.compare(left.pathSegments.length, right.pathSegments.length);
                        return depth != 0 ? depth : Integer.compare(left.rowIndex, right.rowIndex);
                    }
                });

        if (documentIdFieldName != null && idField == null) {
            throw new IllegalArgumentException(
                    "document_id_field does not exist in source schema: " + documentIdFieldName);
        }
        this.fields = Collections.unmodifiableList(prepared);
        this.documentIdField = idField;
    }

    public BsonDocument convert(FluxRow row) {
        Objects.requireNonNull(row, "row must not be null");
        if (row.getArity() != fields.size()) {
            throw new IllegalArgumentException(
                    "MongoDB Sink row arity does not match prepared schema: expected="
                            + fields.size()
                            + ", actual="
                            + row.getArity());
        }

        BsonDocument document = new BsonDocument();
        for (FieldConversion field : fields) {
            Object raw = row.getField(field.rowIndex);
            BsonValue value = convertValue(field, raw);

            if (field.isMongoId() && value.getBsonType() == BsonType.NULL) {
                if (documentIdField == field) {
                    throw new IllegalArgumentException(
                            "document_id_field must not be null: " + field.column.getName());
                }
                continue;
            }
            putPath(document, field.pathSegments, value, field.column.getName());
        }

        if (documentIdField != null && !documentIdField.isMongoId()) {
            BsonValue idValue = convertValue(
                    documentIdField,
                    row.getField(documentIdField.rowIndex));
            validateDocumentId(idValue, documentIdField.column.getName());
            document.put("_id", idValue);
        } else if (documentIdField != null) {
            validateDocumentId(document.get("_id"), documentIdField.column.getName());
        } else if (document.containsKey("_id")) {
            validateDocumentId(document.get("_id"), "_id");
        }

        return document;
    }

    private static BsonValue convertValue(FieldConversion field, Object raw) {
        if (raw == null) {
            return BsonNull.VALUE;
        }

        switch (field.sqlType) {
            case STRING:
                return convertString(field, raw);
            case BOOLEAN:
                return new BsonBoolean(require(Boolean.class, raw, field));
            case TINYINT:
                return new BsonInt32(require(Byte.class, raw, field).intValue());
            case SMALLINT:
                return new BsonInt32(require(Short.class, raw, field).intValue());
            case INT:
                return new BsonInt32(require(Integer.class, raw, field));
            case BIGINT:
                return new BsonInt64(require(Long.class, raw, field));
            case FLOAT:
                return new BsonDouble(require(Float.class, raw, field).doubleValue());
            case DOUBLE:
                return new BsonDouble(require(Double.class, raw, field));
            case DECIMAL:
                return toDecimal128(raw, field);
            case BYTES:
                return new BsonBinary(require(byte[].class, raw, field));
            case DATE:
                return new BsonString(require(LocalDate.class, raw, field).toString());
            case TIME:
                return new BsonString(require(LocalTime.class, raw, field).toString());
            case TIMESTAMP:
                return toDateTime(raw, field);
            case TIMESTAMP_TZ:
                return new BsonString(require(OffsetDateTime.class, raw, field).toString());
            case NULL:
                throw new IllegalArgumentException(
                        "MongoDB Sink received a non-null value for NULL field "
                                + field.column.getName());
            default:
                throw new IllegalArgumentException(
                        "MongoDB Sink does not support Link-Up type "
                                + field.sqlType
                                + " for field "
                                + field.column.getName());
        }
    }

    private static BsonValue convertString(FieldConversion field, Object raw) {
        String value = require(String.class, raw, field);
        if (field.restoreObjectId) {
            if (!ObjectId.isValid(value)) {
                throw new IllegalArgumentException(
                        "MongoDB ObjectId source field is not a valid 24-hex value: field="
                                + field.column.getName());
            }
            return new BsonObjectId(new ObjectId(value));
        }
        if (field.restoreExtendedJson) {
            try {
                BsonDocument wrapper = BsonDocument.parse("{\"_value\":" + value + "}");
                return wrapper.get("_value");
            } catch (RuntimeException failure) {
                throw new IllegalArgumentException(
                        "MongoDB Extended JSON source field cannot be restored: field="
                                + field.column.getName(),
                        failure);
            }
        }
        return new BsonString(value);
    }

    private static BsonValue toDecimal128(Object raw, FieldConversion field) {
        BigDecimal decimal = require(BigDecimal.class, raw, field);
        try {
            return new BsonDecimal128(new Decimal128(decimal));
        } catch (RuntimeException failure) {
            throw new IllegalArgumentException(
                    "Decimal value cannot be represented by MongoDB Decimal128: field="
                            + field.column.getName()
                            + ", value="
                            + decimal,
                    failure);
        }
    }

    private static BsonValue toDateTime(Object raw, FieldConversion field) {
        LocalDateTime timestamp = require(LocalDateTime.class, raw, field);
        if (timestamp.getNano() % 1_000_000 != 0) {
            throw new IllegalArgumentException(
                    "MongoDB DateTime has millisecond precision; refusing lossy TIMESTAMP write: field="
                            + field.column.getName()
                            + ", value="
                            + timestamp);
        }
        long epochMillis = timestamp.toInstant(ZoneOffset.UTC).toEpochMilli();
        return new BsonDateTime(epochMillis);
    }

    private static <T> T require(
            Class<T> expected,
            Object raw,
            FieldConversion field) {
        if (!expected.isInstance(raw)) {
            throw typeMismatch(field, raw, expected.getName());
        }
        return expected.cast(raw);
    }

    private static IllegalArgumentException typeMismatch(
            FieldConversion field,
            Object raw,
            String expected) {
        return new IllegalArgumentException(
                "MongoDB Sink row value does not match prepared schema: field="
                        + field.column.getName()
                        + ", expected="
                        + expected
                        + ", actual="
                        + raw.getClass().getName());
    }

    private static void putPath(
            BsonDocument root,
            String[] path,
            BsonValue value,
            String fieldName) {
        BsonDocument current = root;
        for (int i = 0; i < path.length - 1; i++) {
            String segment = path[i];
            BsonValue existing = current.get(segment);
            if (existing == null || existing.getBsonType() == BsonType.NULL) {
                BsonDocument child = new BsonDocument();
                current.put(segment, child);
                current = child;
                continue;
            }
            if (existing.getBsonType() != BsonType.DOCUMENT) {
                throw new IllegalArgumentException(
                        "MongoDB target path conflicts with a non-document parent: field=" + fieldName);
            }
            current = existing.asDocument();
        }
        current.put(path[path.length - 1], value);
    }

    private static void validateDocumentId(BsonValue value, String fieldName) {
        if (value == null || value.getBsonType() == BsonType.NULL) {
            throw new IllegalArgumentException("MongoDB _id must not be null: field=" + fieldName);
        }
        if (value.getBsonType() == BsonType.ARRAY
                || value.getBsonType() == BsonType.DOCUMENT) {
            throw new IllegalArgumentException(
                    "MongoDB Sink keeps document identity scalar; complex _id is not supported: field="
                            + fieldName);
        }
    }

    private static final class FieldConversion {

        private final Column column;
        private final int rowIndex;
        private final String[] pathSegments;
        private final SqlType sqlType;
        private final boolean restoreObjectId;
        private final boolean restoreExtendedJson;

        private FieldConversion(Column column, int rowIndex) {
            this.column = Objects.requireNonNull(column, "column must not be null");
            this.rowIndex = rowIndex;
            this.pathSegments = column.getName().split("\\.");
            this.sqlType = column.getDataType().getSqlType();

            Map<String, String> attributes = column.getAttributes();
            String bsonTypes = normalize(attributes.get("mongodb.bsonTypes"));
            String sourceType = normalize(column.getSourceType());
            this.restoreObjectId =
                    "OBJECT_ID".equals(bsonTypes)
                            || "OBJECTID".equals(sourceType);

            String encoding = normalize(attributes.get("mongodb.encoding"));
            String heterogeneous = normalize(attributes.get("mongodb.heterogeneous"));
            this.restoreExtendedJson =
                    "EXTENDED-JSON".equals(encoding)
                            && !"TRUE".equals(heterogeneous);
        }

        private boolean isMongoId() {
            return pathSegments.length == 1 && "_id".equals(pathSegments[0]);
        }

        private static String normalize(String value) {
            if (value == null) {
                return null;
            }
            String normalized = value.trim();
            return normalized.isEmpty() ? null : normalized.toUpperCase(java.util.Locale.ROOT);
        }
    }
}
