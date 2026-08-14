package com.link.up.connector.http.schema;

import com.link.up.api.table.catalog.Column;
import com.link.up.api.table.catalog.TableSchema;
import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.DecimalType;
import com.link.up.api.table.type.FluxDataType;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 将用户定义的 Schema 字段映射解析为 {@link TableSchema}。
 *
 * <p>支持的类型名（不区分大小写）：
 * <ul>
 *   <li>string</li>
 *   <li>boolean</li>
 *   <li>tinyint / smallint / int / bigint</li>
 *   <li>float / double</li>
 *   <li>decimal(p,s)</li>
 *   <li>date / time / timestamp</li>
 *   <li>bytes</li>
 * </ul>
 */
public final class HttpSchemaParser {

    private static final Pattern DECIMAL_PATTERN =
            Pattern.compile("decimal\\s*\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)",
                    Pattern.CASE_INSENSITIVE);

    private static final Map<String, FluxDataType<?>> TYPE_MAP =
            new LinkedHashMap<>();

    static {
        TYPE_MAP.put("string", BasicType.STRING_TYPE);
        TYPE_MAP.put("boolean", BasicType.BOOLEAN_TYPE);
        TYPE_MAP.put("tinyint", BasicType.BYTE_TYPE);
        TYPE_MAP.put("smallint", BasicType.SHORT_TYPE);
        TYPE_MAP.put("int", BasicType.INT_TYPE);
        TYPE_MAP.put("integer", BasicType.INT_TYPE);
        TYPE_MAP.put("bigint", BasicType.LONG_TYPE);
        TYPE_MAP.put("long", BasicType.LONG_TYPE);
        TYPE_MAP.put("float", BasicType.FLOAT_TYPE);
        TYPE_MAP.put("double", BasicType.DOUBLE_TYPE);
        TYPE_MAP.put("date", BasicType.DATE_TYPE);
        TYPE_MAP.put("time", BasicType.TIME_TYPE);
        TYPE_MAP.put("timestamp", BasicType.TIMESTAMP_TYPE);
        TYPE_MAP.put("bytes", BasicType.BYTES_TYPE);
    }

    private HttpSchemaParser() {
    }

    /**
     * 解析用户定义的 Schema 字段映射为 TableSchema。
     *
     * @param schemaFields key=字段名，value=类型名或类型描述
     * @return 解析后的 TableSchema
     */
    public static TableSchema parse(Map<String, Object> schemaFields) {
        Objects.requireNonNull(schemaFields, "schemaFields must not be null");

        if (schemaFields.isEmpty()) {
            throw new IllegalArgumentException(
                    "HTTP Source schema must define at least one field");
        }

        TableSchema.Builder builder = TableSchema.builder();

        for (Map.Entry<String, Object> entry : schemaFields.entrySet()) {
            String fieldName = entry.getKey();
            String typeName = String.valueOf(entry.getValue()).trim();

            builder.column(
                    Column.builder(fieldName, resolveType(typeName))
                            .nullable(true)
                            .sourceType(typeName)
                            .build());
        }

        return builder.build();
    }

    private static FluxDataType<?> resolveType(String typeName) {
        if (typeName == null || typeName.trim().isEmpty()) {
            throw new IllegalArgumentException("Field type must not be blank");
        }

        String normalized = typeName.trim().toLowerCase();

        // decimal(p,s)
        Matcher matcher = DECIMAL_PATTERN.matcher(normalized);
        if (matcher.matches()) {
            int precision = Integer.parseInt(matcher.group(1));
            int scale = Integer.parseInt(matcher.group(2));
            return new DecimalType(precision, scale);
        }

        FluxDataType<?> type = TYPE_MAP.get(normalized);
        if (type == null) {
            throw new IllegalArgumentException(
                    "Unsupported HTTP schema type: " + typeName
                            + ". Supported types: " + TYPE_MAP.keySet());
        }
        return type;
    }
}
