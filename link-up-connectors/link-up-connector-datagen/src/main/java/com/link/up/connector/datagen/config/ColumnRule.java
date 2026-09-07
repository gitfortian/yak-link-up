package com.link.up.connector.datagen.config;

import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.api.table.type.FluxDataType;
import com.link.up.api.table.type.SqlType;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * One resolved schema column with its generator rule.
 *
 * <p>The rule is parsed and fully validated at configuration time so the
 * generator can stay a pure function of the global row index. Supported types
 * are the Flux scalar types listed in the DataGen design type matrix;
 * anything else fails here instead of at read time.
 */
public final class ColumnRule {

    public enum GeneratorMode {
        PICK,
        SEQUENCE,
        RANGE
    }

    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    private static final LocalDate DATE_ORIGIN = LocalDate.of(2024, 1, 1);
    private static final long DATE_SPAN_DAYS = 3650L;
    private static final LocalDateTime TIMESTAMP_ORIGIN = LocalDateTime.of(2024, 1, 1, 0, 0, 0);
    private static final long TIMESTAMP_SPAN_SECONDS = 3650L * 24L * 60L * 60L;

    private final String name;
    private final SqlType sqlType;
    private final FluxDataType<?> dataType;
    private final boolean nullable;
    private final GeneratorMode mode;
    private final List<Object> pickValues;
    private final long sequenceStart;
    private final long longMin;
    private final long longMax;
    private final double doubleMin;
    private final double doubleMax;
    private final BigDecimal decimalMin;
    private final BigDecimal decimalMax;
    private final int stringLength;
    private final int precision;
    private final int scale;

    private ColumnRule(Builder builder) {
        this.name = builder.name;
        this.sqlType = builder.sqlType;
        this.dataType = builder.dataType;
        this.nullable = builder.nullable;
        this.mode = builder.mode;
        this.pickValues = builder.pickValues == null
                ? Collections.<Object>emptyList()
                : Collections.unmodifiableList(builder.pickValues);
        this.sequenceStart = builder.sequenceStart;
        this.longMin = builder.longMin;
        this.longMax = builder.longMax;
        this.doubleMin = builder.doubleMin;
        this.doubleMax = builder.doubleMax;
        this.decimalMin = builder.decimalMin;
        this.decimalMax = builder.decimalMax;
        this.stringLength = builder.stringLength;
        this.precision = builder.precision;
        this.scale = builder.scale;
    }

    /**
     * Parses raw schema entries into resolved column rules.
     *
     * @param rawSchema  raw {@code schema} option entries
     * @param totalRows  total row count the generator will produce; used to
     *                   bound sequence columns inside the column type
     * @param presetRows true when explicit preset rows replace generation
     */
    public static List<ColumnRule> parseSchema(
            List<Map> rawSchema,
            long totalRows,
            boolean presetRows) {

        if (rawSchema == null || rawSchema.isEmpty()) {
            throw new IllegalArgumentException("schema must not be empty");
        }

        Set<String> seenNames = new HashSet<String>();
        List<Builder> builders = new ArrayList<Builder>();

        for (int i = 0; i < rawSchema.size(); i++) {
            Object entry = rawSchema.get(i);
            if (!(entry instanceof Map)) {
                throw new IllegalArgumentException(
                        "schema entry #" + i + " must be an object with name/type fields");
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> raw = (Map<String, Object>) entry;
            Builder builder = Builder.parse(i, raw);
            if (!seenNames.add(builder.name)) {
                throw new IllegalArgumentException("Duplicate DataGen column: " + builder.name);
            }
            builders.add(builder);
        }

        List<ColumnRule> rules = new ArrayList<ColumnRule>(builders.size());
        for (Builder builder : builders) {
            rules.add(builder.build(totalRows, presetRows));
        }
        return rules;
    }

    /** Converts one preset row cell to the column's physical value. */
    public Object convertPresetCell(int columnIndex, Object cell) {
        if (cell == null) {
            return null;
        }
        String text = String.valueOf(cell);
        try {
            switch (sqlType) {
                case STRING:
                    return text;
                case BOOLEAN:
                    if ("true".equals(text) || "false".equals(text)) {
                        return Boolean.valueOf(text);
                    }
                    throw new IllegalArgumentException("expected true/false");
                case TINYINT:
                    return Byte.valueOf(text);
                case SMALLINT:
                    return Short.valueOf(text);
                case INT:
                    return Integer.valueOf(text);
                case BIGINT:
                    return Long.valueOf(text);
                case FLOAT:
                    return Float.valueOf(text);
                case DOUBLE:
                    return Double.valueOf(text);
                case DECIMAL:
                    return new BigDecimal(text);
                case BYTES:
                    return Base64.getDecoder().decode(text);
                case DATE:
                    return LocalDate.parse(text);
                case TIME:
                    return LocalTime.parse(text, TIME_FORMAT);
                case TIMESTAMP:
                    return LocalDateTime.parse(text, TIMESTAMP_FORMAT);
                default:
                    throw new IllegalArgumentException("unsupported preset type");
            }
        } catch (RuntimeException failure) {
            throw new IllegalArgumentException(
                    "Cannot convert preset value '" + text + "' for column '"
                            + name + "' of type " + sqlType,
                    failure);
        }
    }

    public String getName() {
        return name;
    }

    public SqlType getSqlType() {
        return sqlType;
    }

    public FluxDataType<?> getDataType() {
        return dataType;
    }

    public boolean isNullable() {
        return nullable;
    }

    public GeneratorMode getMode() {
        return mode;
    }

    public List<Object> getPickValues() {
        return pickValues;
    }

    public long getSequenceStart() {
        return sequenceStart;
    }

    public long getLongMin() {
        return longMin;
    }

    public long getLongMax() {
        return longMax;
    }

    public double getDoubleMin() {
        return doubleMin;
    }

    public double getDoubleMax() {
        return doubleMax;
    }

    public BigDecimal getDecimalMin() {
        return decimalMin;
    }

    public BigDecimal getDecimalMax() {
        return decimalMax;
    }

    public int getStringLength() {
        return stringLength;
    }

    public int getScale() {
        return scale;
    }

    public int getPrecision() {
        return precision;
    }

    private static final class Builder {

        private static final String SUPPORTED_TYPES =
                "string, boolean, tinyint, smallint, int, bigint, float, double, "
                        + "decimal(p,s), bytes, date, time, timestamp";

        private String name;
        private SqlType sqlType;
        private FluxDataType<?> dataType;
        private boolean nullable = true;
        private GeneratorMode mode = GeneratorMode.RANGE;
        private List<Object> pickValues;
        private long sequenceStart;
        private Long longMin;
        private Long longMax;
        private Double doubleMin;
        private Double doubleMax;
        private BigDecimal decimalMin;
        private BigDecimal decimalMax;
        private int stringLength = 8;
        private int precision;
        private int scale;

        private static Builder parse(int index, Map<String, Object> raw) {
            Builder builder = new Builder();

            Object rawName = raw.get("name");
            if (!(rawName instanceof String) || ((String) rawName).trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "schema entry #" + index + " must declare a non-blank name");
            }
            builder.name = ((String) rawName).trim();

            Object rawType = raw.get("type");
            if (!(rawType instanceof String) || ((String) rawType).trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "Column '" + builder.name + "' must declare a type; supported: " + SUPPORTED_TYPES);
            }
            builder.parseType(((String) rawType).trim());

            Object rawNullable = raw.get("nullable");
            if (rawNullable != null) {
                builder.nullable = Boolean.parseBoolean(String.valueOf(rawNullable));
            }

            Object rawMin = raw.get("min");
            Object rawMax = raw.get("max");
            Object rawLength = raw.get("length");
            Object rawValues = raw.get("values");
            Object rawSequenceStart = raw.get("sequence_start");

            if (rawValues != null && !(rawValues instanceof List)) {
                throw new IllegalArgumentException(
                        "Column '" + builder.name + "' values must be a list");
            }

            if ((rawMin != null && rawMax == null) || (rawMin == null && rawMax != null)) {
                throw new IllegalArgumentException(
                        "Column '" + builder.name + "' must declare min and max together");
            }

            // min/max travel together and count as one hint.
            int hints = 0;
            if (rawMin != null) {
                hints++;
            }
            if (rawLength != null) {
                hints++;
            }
            if (rawValues != null) {
                hints++;
            }
            if (rawSequenceStart != null) {
                hints++;
            }
            if (hints > 1) {
                throw new IllegalArgumentException(
                        "Column '" + builder.name + "' declares conflicting generator hints; "
                                + "use at most one of min/max, length, values, sequence_start");
            }

            if (rawValues != null) {
                builder.mode = GeneratorMode.PICK;
                builder.parsePickValues((List<?>) rawValues);
            } else if (rawSequenceStart != null) {
                builder.mode = GeneratorMode.SEQUENCE;
                builder.sequenceStart = parseLong(builder.name, "sequence_start", rawSequenceStart);
            } else {
                builder.mode = GeneratorMode.RANGE;
                if (rawMin != null) {
                    builder.parseRange(builder.name, rawMin, rawMax);
                } else if (rawLength != null) {
                    if (builder.sqlType != SqlType.STRING) {
                        throw new IllegalArgumentException(
                                "Column '" + builder.name + "' length is only supported for string columns");
                    }
                    long parsed = parseLong(builder.name, "length", rawLength);
                    if (parsed < 0) {
                        throw new IllegalArgumentException(
                                "Column '" + builder.name + "' length must not be negative");
                    }
                    builder.stringLength = (int) parsed;
                }
            }

            return builder;
        }

        private void parseType(String type) {
            String normalized = type.toLowerCase(java.util.Locale.ROOT);
            if (normalized.startsWith("decimal")) {
                parseDecimalType(normalized);
                return;
            }

            switch (normalized) {
                case "string":
                    sqlType = SqlType.STRING;
                    dataType = BasicType.STRING_TYPE;
                    break;
                case "boolean":
                    sqlType = SqlType.BOOLEAN;
                    dataType = BasicType.BOOLEAN_TYPE;
                    break;
                case "tinyint":
                    sqlType = SqlType.TINYINT;
                    dataType = BasicType.BYTE_TYPE;
                    break;
                case "smallint":
                    sqlType = SqlType.SMALLINT;
                    dataType = BasicType.SHORT_TYPE;
                    break;
                case "int":
                    sqlType = SqlType.INT;
                    dataType = BasicType.INT_TYPE;
                    break;
                case "bigint":
                    sqlType = SqlType.BIGINT;
                    dataType = BasicType.LONG_TYPE;
                    break;
                case "float":
                    sqlType = SqlType.FLOAT;
                    dataType = BasicType.FLOAT_TYPE;
                    break;
                case "double":
                    sqlType = SqlType.DOUBLE;
                    dataType = BasicType.DOUBLE_TYPE;
                    break;
                case "bytes":
                    sqlType = SqlType.BYTES;
                    dataType = BasicType.BYTES_TYPE;
                    break;
                case "date":
                    sqlType = SqlType.DATE;
                    dataType = BasicType.DATE_TYPE;
                    break;
                case "time":
                    sqlType = SqlType.TIME;
                    dataType = BasicType.TIME_TYPE;
                    break;
                case "timestamp":
                    sqlType = SqlType.TIMESTAMP;
                    dataType = BasicType.TIMESTAMP_TYPE;
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Unsupported DataGen column type '" + type + "' for column '"
                                    + name + "'; supported: " + SUPPORTED_TYPES);
            }
        }

        private void parseDecimalType(String type) {
            int open = type.indexOf('(');
            int close = type.indexOf(')');
            if (open < 0 || close <= open) {
                throw new IllegalArgumentException(
                        "Column '" + name + "' decimal type requires precision and scale: decimal(p,s)");
            }

            String[] parts = type.substring(open + 1, close).split(",");
            if (parts.length != 2) {
                throw new IllegalArgumentException(
                        "Column '" + name + "' decimal type requires precision and scale: decimal(p,s)");
            }

            try {
                precision = Integer.parseInt(parts[0].trim());
                scale = Integer.parseInt(parts[1].trim());
            } catch (NumberFormatException failure) {
                throw new IllegalArgumentException(
                        "Column '" + name + "' has an invalid decimal(p,s) declaration: " + type,
                        failure);
            }

            try {
                dataType = new DecimalType(precision, scale);
            } catch (IllegalArgumentException failure) {
                throw new IllegalArgumentException(
                        "Column '" + name + "' has an invalid decimal type: " + type,
                        failure);
            }
            sqlType = SqlType.DECIMAL;
        }

        private void parsePickValues(List<?> rawValues) {
            if (rawValues.isEmpty()) {
                throw new IllegalArgumentException(
                        "Column '" + name + "' values must not be empty");
            }
            if (sqlType == SqlType.DECIMAL) {
                throw new IllegalArgumentException(
                        "Column '" + name + "' of type decimal supports min/max range only");
            }

            List<Object> converted = new ArrayList<Object>(rawValues.size());
            for (Object cell : rawValues) {
                converted.add(cell == null ? null : convertCell(String.valueOf(cell)));
            }
            pickValues = converted;
        }

        private void parseRange(String columnName, Object rawMin, Object rawMax) {
            switch (sqlType) {
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                    longMin = parseLong(columnName, "min", rawMin);
                    longMax = parseLong(columnName, "max", rawMax);
                    validateLongBounds(columnName);
                    if (longMin > longMax) {
                        throw new IllegalArgumentException(
                                "Column '" + columnName + "' min must not be greater than max");
                    }
                    break;
                case FLOAT:
                case DOUBLE:
                    doubleMin = parseDouble(columnName, "min", rawMin);
                    doubleMax = parseDouble(columnName, "max", rawMax);
                    if (doubleMin > doubleMax) {
                        throw new IllegalArgumentException(
                                "Column '" + columnName + "' min must not be greater than max");
                    }
                    break;
                case DECIMAL:
                    decimalMin = parseDecimal(columnName, "min", rawMin);
                    decimalMax = parseDecimal(columnName, "max", rawMax);
                    validateDecimalBounds(columnName, decimalMin, "min");
                    validateDecimalBounds(columnName, decimalMax, "max");
                    if (decimalMin.compareTo(decimalMax) > 0) {
                        throw new IllegalArgumentException(
                                "Column '" + columnName + "' min must not be greater than max");
                    }
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Column '" + columnName + "' of type " + sqlType
                                    + " does not support min/max; use values instead");
            }
        }

        private Object convertCell(String text) {
            switch (sqlType) {
                case STRING:
                    return text;
                case BOOLEAN:
                    if ("true".equals(text) || "false".equals(text)) {
                        return Boolean.valueOf(text);
                    }
                    throw new IllegalArgumentException("expected true/false");
                case TINYINT:
                    return Byte.valueOf(text);
                case SMALLINT:
                    return Short.valueOf(text);
                case INT:
                    return Integer.valueOf(text);
                case BIGINT:
                    return Long.valueOf(text);
                case FLOAT:
                    return Float.valueOf(text);
                case DOUBLE:
                    return Double.valueOf(text);
                case DATE:
                    return LocalDate.parse(text);
                case TIME:
                    return LocalTime.parse(text, TIME_FORMAT);
                case TIMESTAMP:
                    return LocalDateTime.parse(text, TIMESTAMP_FORMAT);
                default:
                    throw new IllegalArgumentException("unsupported pick type");
            }
        }

        private void validateLongBounds(String columnName) {
            checkLongBound(columnName, "min", longMin);
            checkLongBound(columnName, "max", longMax);
        }

        private void checkLongBound(String columnName, String label, long value) {
            boolean within;
            switch (sqlType) {
                case TINYINT:
                    within = value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE;
                    break;
                case SMALLINT:
                    within = value >= Short.MIN_VALUE && value <= Short.MAX_VALUE;
                    break;
                case INT:
                    within = value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE;
                    break;
                default:
                    within = true;
            }
            if (!within) {
                throw new IllegalArgumentException(
                        "Column '" + columnName + "' " + label + "=" + value
                                + " is outside the " + sqlType + " range");
            }
        }

        private void validateDecimalBounds(String columnName, BigDecimal value, String label) {
            if (value.scale() > scale) {
                throw new IllegalArgumentException(
                        "Column '" + columnName + "' " + label + "=" + value.toPlainString()
                                + " has scale " + value.scale() + " beyond the declared scale " + scale);
            }

            BigDecimal unscaled = value.setScale(scale).abs().movePointRight(scale);
            if (unscaled.compareTo(BigDecimal.TEN.pow(precision)) >= 0) {
                throw new IllegalArgumentException(
                        "Column '" + columnName + "' " + label + "=" + value.toPlainString()
                                + " does not fit decimal(" + precision + "," + scale + ")");
            }
        }

        private ColumnRule build(long totalRows, boolean presetRows) {
            if (mode == GeneratorMode.SEQUENCE && !isIntegral()) {
                throw new IllegalArgumentException(
                        "Column '" + name + "' sequence_start is only supported for "
                                + "tinyint/smallint/int/bigint columns");
            }
            if (mode == GeneratorMode.SEQUENCE && !presetRows) {
                long lastValue = sequenceStart + Math.max(0L, totalRows - 1L);
                if (lastValue < sequenceStart || lastValue > typeMax()) {
                    throw new IllegalArgumentException(
                            "Column '" + name + "' sequence overflows " + sqlType
                                    + " within " + totalRows + " rows; lower sequence_start or row_count");
                }
                longMax = lastValue;
                longMin = sequenceStart;
            }
            if (sqlType == SqlType.BYTES && !presetRows) {
                throw new IllegalArgumentException(
                        "Column '" + name + "' of type bytes cannot be generated; "
                                + "provide preset rows with base64 values");
            }
            if (sqlType == SqlType.DECIMAL) {
                if (mode != GeneratorMode.RANGE) {
                    throw new IllegalArgumentException(
                            "Column '" + name + "' of type decimal supports min/max range only");
                }
                if (decimalMin == null) {
                    decimalMin = BigDecimal.ZERO;
                    decimalMax = BigDecimal.valueOf(10000L);
                }
            }
            if (isIntegral() && mode == GeneratorMode.RANGE && longMin == null) {
                longMin = 0L;
                longMax = typeMax();
            }
            if ((sqlType == SqlType.FLOAT || sqlType == SqlType.DOUBLE)
                    && mode == GeneratorMode.RANGE
                    && doubleMin == null) {
                doubleMin = 0.0d;
                doubleMax = 1000.0d;
            }
            // Non-generated kinds never read these fields; normalize so the
            // primitive constructor never unboxes a null.
            if (longMin == null) {
                longMin = 0L;
                longMax = 0L;
            }
            if (doubleMin == null) {
                doubleMin = 0.0d;
                doubleMax = 0.0d;
            }
            return new ColumnRule(this);
        }

        private boolean isIntegral() {
            return sqlType == SqlType.TINYINT
                    || sqlType == SqlType.SMALLINT
                    || sqlType == SqlType.INT
                    || sqlType == SqlType.BIGINT;
        }

        private long typeMax() {
            switch (sqlType) {
                case TINYINT:
                    return Byte.MAX_VALUE;
                case SMALLINT:
                    return Short.MAX_VALUE;
                case INT:
                    return Integer.MAX_VALUE;
                default:
                    return Long.MAX_VALUE;
            }
        }

        private static long parseLong(String columnName, String label, Object raw) {
            try {
                String text = String.valueOf(raw).trim();
                return Long.parseLong(text);
            } catch (NumberFormatException failure) {
                throw new IllegalArgumentException(
                        "Column '" + columnName + "' " + label + " is not a valid integer: " + raw,
                        failure);
            }
        }

        private static double parseDouble(String columnName, String label, Object raw) {
            try {
                String text = String.valueOf(raw).trim();
                return Double.parseDouble(text);
            } catch (NumberFormatException failure) {
                throw new IllegalArgumentException(
                        "Column '" + columnName + "' " + label + " is not a valid number: " + raw,
                        failure);
            }
        }

        private static BigDecimal parseDecimal(String columnName, String label, Object raw) {
            try {
                String text = String.valueOf(raw).trim();
                return new BigDecimal(text);
            } catch (NumberFormatException failure) {
                throw new IllegalArgumentException(
                        "Column '" + columnName + "' " + label + " is not a valid decimal: " + raw,
                        failure);
            }
        }
    }

    @Override
    public String toString() {
        return "ColumnRule{name='" + name + "', type=" + sqlType + ", mode=" + mode + '}';
    }
}
