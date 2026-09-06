package com.link.up.connector.clickhouse.schema;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.api.table.type.FluxDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** Conservative ClickHouse type mapping for bounded source reads. */
public final class ClickHouseTypeMapper {

    private ClickHouseTypeMapper() {
    }

    public static Column toColumn(String name, String sourceType) {
        TypeInfo info = parse(sourceType);
        Column.Builder builder =
                Column.builder(name, info.dataType)
                        .nullable(info.nullable)
                        .sourceType(sourceType);
        if (info.length != null) {
            builder.length(info.length);
        }
        if (info.precision != null) {
            builder.precision(info.precision);
        }
        if (info.scale != null) {
            builder.scale(info.scale);
        }
        return builder.build();
    }

    public static TypeInfo parse(String sourceType) {
        String original = requireText(sourceType, "sourceType");
        String type = original.trim();
        boolean nullable = false;

        boolean changed = true;
        while (changed) {
            changed = false;
            if (isWrapper(type, "Nullable")) {
                nullable = true;
                type = unwrap(type);
                changed = true;
            } else if (isWrapper(type, "LowCardinality")) {
                type = unwrap(type);
                changed = true;
            } else if (isWrapper(type, "SimpleAggregateFunction")) {
                List<String> args = splitTopLevel(unwrap(type));
                if (args.size() < 2) {
                    throw unsupported(original, "invalid SimpleAggregateFunction type");
                }
                type = args.get(args.size() - 1).trim();
                changed = true;
            }
        }

        String upper = type.toUpperCase(Locale.ROOT);
        if ("BOOL".equals(upper) || "BOOLEAN".equals(upper)) {
            return info(BasicType.BOOLEAN_TYPE, nullable);
        }
        if ("INT8".equals(upper)) {
            return info(BasicType.BYTE_TYPE, nullable);
        }
        if ("UINT8".equals(upper) || "INT16".equals(upper)) {
            return info(BasicType.SHORT_TYPE, nullable);
        }
        if ("UINT16".equals(upper) || "INT32".equals(upper)) {
            return info(BasicType.INT_TYPE, nullable);
        }
        if ("UINT32".equals(upper) || "INT64".equals(upper)) {
            return info(BasicType.LONG_TYPE, nullable);
        }
        if ("UINT64".equals(upper)) {
            return decimal(20, 0, nullable);
        }
        if ("INT128".equals(upper)
                || "UINT128".equals(upper)
                || "INT256".equals(upper)
                || "UINT256".equals(upper)) {
            return info(BasicType.STRING_TYPE, nullable);
        }
        if ("FLOAT32".equals(upper)) {
            return info(BasicType.FLOAT_TYPE, nullable);
        }
        if ("FLOAT64".equals(upper)) {
            return info(BasicType.DOUBLE_TYPE, nullable);
        }
        if (upper.startsWith("DECIMAL(")) {
            List<String> args = splitTopLevel(unwrap(type));
            if (args.size() != 2) {
                throw unsupported(original, "DECIMAL requires precision and scale");
            }
            return decimal(parsePositive(args.get(0), "precision"), parseNonNegative(args.get(1), "scale"), nullable);
        }
        if (upper.startsWith("DECIMAL32(")) {
            return decimal(9, parseNonNegative(unwrap(type), "scale"), nullable);
        }
        if (upper.startsWith("DECIMAL64(")) {
            return decimal(18, parseNonNegative(unwrap(type), "scale"), nullable);
        }
        if (upper.startsWith("DECIMAL128(")) {
            return decimal(38, parseNonNegative(unwrap(type), "scale"), nullable);
        }
        if (upper.startsWith("DECIMAL256(")) {
            return decimal(76, parseNonNegative(unwrap(type), "scale"), nullable);
        }
        if ("DATE".equals(upper) || "DATE32".equals(upper)) {
            return info(BasicType.DATE_TYPE, nullable);
        }
        if (upper.equals("DATETIME")
                || upper.startsWith("DATETIME(")
                || upper.startsWith("DATETIME64(")) {
            TypeInfo result = info(BasicType.TIMESTAMP_TYPE, nullable);
            if (upper.startsWith("DATETIME64(")) {
                List<String> args = splitTopLevel(unwrap(type));
                if (!args.isEmpty()) {
                    int precision = parseNonNegative(args.get(0), "DateTime64 precision");
                    if (precision > 9) {
                        throw unsupported(original, "DateTime64 precision greater than 9 is unsupported");
                    }
                    result = result.withPrecision(precision);
                }
            }
            return result;
        }
        if (upper.startsWith("FIXEDSTRING(")) {
            int length = parsePositive(unwrap(type), "FixedString length");
            return info(BasicType.STRING_TYPE, nullable).withLength((long) length);
        }
        if ("STRING".equals(upper)
                || "UUID".equals(upper)
                || "IPV4".equals(upper)
                || "IPV6".equals(upper)
                || upper.startsWith("ENUM8(")
                || upper.startsWith("ENUM16(")
                || "JSON".equals(upper)
                || upper.startsWith("OBJECT(")
                || upper.startsWith("DYNAMIC")
                || upper.startsWith("VARIANT(")
                || "POINT".equals(upper)
                || "RING".equals(upper)
                || "POLYGON".equals(upper)
                || "MULTIPOLYGON".equals(upper)) {
            return info(BasicType.STRING_TYPE, nullable);
        }
        if (upper.startsWith("INTERVAL")) {
            return info(BasicType.LONG_TYPE, nullable);
        }
        if ("NOTHING".equals(upper)) {
            return info(BasicType.NULL_TYPE, true);
        }

        if (upper.startsWith("ARRAY(")
                || upper.startsWith("MAP(")
                || upper.startsWith("TUPLE(")
                || upper.startsWith("NESTED(")
                || upper.startsWith("AGGREGATEFUNCTION(")) {
            throw unsupported(
                    original,
                    "complex/aggregate ClickHouse types are intentionally outside the bounded Source stage");
        }

        throw unsupported(original, "unknown ClickHouse type");
    }

    private static TypeInfo decimal(int precision, int scale, boolean nullable) {
        if (scale > precision) {
            throw new IllegalArgumentException(
                    "ClickHouse decimal scale must not exceed precision: " + precision + "," + scale);
        }
        return new TypeInfo(new DecimalType(precision, scale), nullable, null, precision, scale);
    }

    private static TypeInfo info(FluxDataType<?> dataType, boolean nullable) {
        return new TypeInfo(dataType, nullable, null, null, null);
    }

    private static boolean isWrapper(String value, String wrapper) {
        String trimmed = value.trim();
        return trimmed.regionMatches(true, 0, wrapper + "(", 0, wrapper.length() + 1)
                && trimmed.endsWith(")");
    }

    private static String unwrap(String value) {
        int start = value.indexOf('(');
        int end = value.lastIndexOf(')');
        if (start < 0 || end <= start) {
            throw new IllegalArgumentException("Invalid ClickHouse type expression: " + value);
        }
        return value.substring(start + 1, end).trim();
    }

    private static List<String> splitTopLevel(String value) {
        List<String> result = new ArrayList<String>();
        int depth = 0;
        boolean quoted = false;
        char quote = 0;
        int start = 0;
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (quoted) {
                if (ch == quote && (i == 0 || value.charAt(i - 1) != '\\')) {
                    quoted = false;
                }
                continue;
            }
            if (ch == '\'' || ch == '"') {
                quoted = true;
                quote = ch;
            } else if (ch == '(') {
                depth++;
            } else if (ch == ')') {
                depth--;
            } else if (ch == ',' && depth == 0) {
                result.add(value.substring(start, i).trim());
                start = i + 1;
            }
        }
        result.add(value.substring(start).trim());
        return result;
    }

    private static int parsePositive(String value, String name) {
        int parsed = parseInteger(value, name);
        if (parsed <= 0) {
            throw new IllegalArgumentException(name + " must be greater than 0");
        }
        return parsed;
    }

    private static int parseNonNegative(String value, String name) {
        int parsed = parseInteger(value, name);
        if (parsed < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return parsed;
    }

    private static int parseInteger(String value, String name) {
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(name + " must be an integer: " + value, failure);
        }
    }

    private static IllegalArgumentException unsupported(String sourceType, String detail) {
        return new IllegalArgumentException(
                "Unsupported ClickHouse Source type '" + sourceType + "': " + detail);
    }

    private static String requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }

    public static final class TypeInfo {
        private final FluxDataType<?> dataType;
        private final boolean nullable;
        private final Long length;
        private final Integer precision;
        private final Integer scale;

        private TypeInfo(
                FluxDataType<?> dataType,
                boolean nullable,
                Long length,
                Integer precision,
                Integer scale) {
            this.dataType = dataType;
            this.nullable = nullable;
            this.length = length;
            this.precision = precision;
            this.scale = scale;
        }

        private TypeInfo withLength(Long value) {
            return new TypeInfo(dataType, nullable, value, precision, scale);
        }

        private TypeInfo withPrecision(Integer value) {
            return new TypeInfo(dataType, nullable, length, value, scale);
        }

        public FluxDataType<?> getDataType() {
            return dataType;
        }

        public boolean isNullable() {
            return nullable;
        }

        public Long getLength() {
            return length;
        }

        public Integer getPrecision() {
            return precision;
        }

        public Integer getScale() {
            return scale;
        }
    }
}
