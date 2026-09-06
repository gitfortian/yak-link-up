package com.link.up.connector.mongodb.converter;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.FluxRow;
import com.link.up.api.table.type.SqlType;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Converts one BSON document to a FluxRow using the prepared discovered schema. */
public final class MongoBsonRowConverter {

    private final List<FieldConversion> fields;

    public MongoBsonRowConverter(TableSchema schema) {
        Objects.requireNonNull(schema, "schema must not be null");
        List<FieldConversion> prepared = new ArrayList<FieldConversion>(schema.getColumnCount());
        for (Column column : schema.getColumns()) {
            prepared.add(new FieldConversion(column));
        }
        this.fields = prepared;
    }

    public FluxRow convert(BsonDocument document) {
        Objects.requireNonNull(document, "document must not be null");
        FluxRow row = new FluxRow(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            FieldConversion field = fields.get(i);
            BsonValue value = extract(document, field.pathSegments);
            row.setField(i, convertValue(field, value));
        }
        return row;
    }

    private static BsonValue extract(BsonDocument document, String[] pathSegments) {
        BsonDocument current = document;
        for (int i = 0; i < pathSegments.length; i++) {
            BsonValue value = current.get(pathSegments[i]);
            if (value == null) {
                return null;
            }
            if (i == pathSegments.length - 1) {
                return value;
            }
            if (value.getBsonType() != BsonType.DOCUMENT) {
                return null;
            }
            current = value.asDocument();
        }
        return null;
    }

    private static Object convertValue(FieldConversion field, BsonValue value) {
        if (value == null || value.getBsonType() == BsonType.NULL) {
            return null;
        }

        switch (field.sqlType) {
            case STRING:
                return toPortableString(value);
            case BOOLEAN:
                requireType(field, value, BsonType.BOOLEAN);
                return value.asBoolean().getValue();
            case TINYINT:
                return toByte(field, value);
            case SMALLINT:
                return toShort(field, value);
            case INT:
                requireType(field, value, BsonType.INT32);
                return value.asInt32().getValue();
            case BIGINT:
                return toLong(field, value);
            case FLOAT:
                return toFloat(field, value);
            case DOUBLE:
                return toDouble(field, value);
            case DECIMAL:
                return toDecimal(field, value);
            case BYTES:
                requireType(field, value, BsonType.BINARY);
                byte[] bytes = value.asBinary().getData();
                return Arrays.copyOf(bytes, bytes.length);
            case TIMESTAMP:
                return toTimestamp(field, value);
            default:
                throw unsupported(field, value);
        }
    }

    private static Byte toByte(FieldConversion field, BsonValue value) {
        requireType(field, value, BsonType.INT32);
        int number = value.asInt32().getValue();
        if (number < Byte.MIN_VALUE || number > Byte.MAX_VALUE) {
            throw typeMismatch(field, value, "value is outside TINYINT range");
        }
        return (byte) number;
    }

    private static Short toShort(FieldConversion field, BsonValue value) {
        requireType(field, value, BsonType.INT32);
        int number = value.asInt32().getValue();
        if (number < Short.MIN_VALUE || number > Short.MAX_VALUE) {
            throw typeMismatch(field, value, "value is outside SMALLINT range");
        }
        return (short) number;
    }

    private static Long toLong(FieldConversion field, BsonValue value) {
        if (value.getBsonType() == BsonType.INT32) {
            return (long) value.asInt32().getValue();
        }
        if (value.getBsonType() == BsonType.INT64) {
            return value.asInt64().getValue();
        }
        throw typeMismatch(field, value, "expected Int32 or Int64");
    }

    private static Float toFloat(FieldConversion field, BsonValue value) {
        switch (value.getBsonType()) {
            case INT32:
                return (float) value.asInt32().getValue();
            case INT64:
                return (float) value.asInt64().getValue();
            case DOUBLE:
                return (float) value.asDouble().getValue();
            default:
                throw typeMismatch(field, value, "expected a BSON numeric value");
        }
    }

    private static Double toDouble(FieldConversion field, BsonValue value) {
        switch (value.getBsonType()) {
            case INT32:
                return (double) value.asInt32().getValue();
            case INT64:
                return (double) value.asInt64().getValue();
            case DOUBLE:
                return value.asDouble().getValue();
            default:
                throw typeMismatch(field, value, "expected Int32, Int64, or Double");
        }
    }

    private static BigDecimal toDecimal(FieldConversion field, BsonValue value) {
        switch (value.getBsonType()) {
            case INT32:
                return BigDecimal.valueOf(value.asInt32().getValue());
            case INT64:
                return BigDecimal.valueOf(value.asInt64().getValue());
            case DECIMAL128:
                try {
                    return value.asDecimal128().getValue().bigDecimalValue();
                } catch (ArithmeticException failure) {
                    throw typeMismatch(field, value, "Decimal128 value is not a finite BigDecimal");
                }
            default:
                throw typeMismatch(field, value, "expected Int32, Int64, or Decimal128");
        }
    }

    private static LocalDateTime toTimestamp(FieldConversion field, BsonValue value) {
        final Instant instant;
        if (value.getBsonType() == BsonType.DATE_TIME) {
            instant = Instant.ofEpochMilli(value.asDateTime().getValue());
        } else if (value.getBsonType() == BsonType.TIMESTAMP) {
            instant = Instant.ofEpochSecond(value.asTimestamp().getTime());
        } else {
            throw typeMismatch(field, value, "expected DateTime or Timestamp");
        }
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static String toPortableString(BsonValue value) {
        if (value.getBsonType() == BsonType.STRING) {
            return value.asString().getValue();
        }
        if (value.getBsonType() == BsonType.OBJECT_ID) {
            return value.asObjectId().getValue().toHexString();
        }
        return toExtendedJsonValue(value);
    }

    private static String toExtendedJsonValue(BsonValue value) {
        String wrapped = new BsonDocument("_value", value).toJson();
        int separator = wrapped.indexOf(':');
        if (separator < 0 || wrapped.length() < separator + 2) {
            return value.toString();
        }
        return wrapped.substring(separator + 1, wrapped.length() - 1).trim();
    }

    private static void requireType(
            FieldConversion field,
            BsonValue value,
            BsonType expected) {
        if (value.getBsonType() != expected) {
            throw typeMismatch(field, value, "expected " + expected);
        }
    }

    private static IllegalArgumentException unsupported(
            FieldConversion field,
            BsonValue value) {
        return typeMismatch(
                field,
                value,
                "Link-Up type " + field.sqlType + " is not supported by the MongoDB Source converter");
    }

    private static IllegalArgumentException typeMismatch(
            FieldConversion field,
            BsonValue value,
            String detail) {
        return new IllegalArgumentException(
                "MongoDB field type changed after schema discovery, field="
                        + field.name
                        + ", expected="
                        + field.sqlType
                        + ", actual="
                        + value.getBsonType()
                        + ", detail="
                        + detail);
    }

    private static final class FieldConversion {

        private final String name;
        private final String[] pathSegments;
        private final SqlType sqlType;

        private FieldConversion(Column column) {
            this.name = column.getName();
            String physicalPath = column.getAttributes().get("mongodb.path");
            if (physicalPath == null || physicalPath.trim().isEmpty()) {
                physicalPath = column.getName();
            }
            this.pathSegments = physicalPath.split("\\.");
            this.sqlType = column.getDataType().getSqlType();
        }
    }
}
