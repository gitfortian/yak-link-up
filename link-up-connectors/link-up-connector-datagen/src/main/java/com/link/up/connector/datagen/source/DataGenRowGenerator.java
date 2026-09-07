package com.link.up.connector.datagen.source;

import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.datagen.config.ColumnRule;
import com.link.up.connector.datagen.config.DataGenSourceConfig;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Objects;
import java.util.Random;

/**
 * Pure row generator: the same seed and global row index always produce the
 * same row, no matter how splits or batches distribute the work.
 *
 * <p>Randomness is derived per row from {@code seed ^ row * GOLDEN}, so two
 * readers generating different ranges never collide and the produced data set
 * is independent of split assignment. Without a seed one shared Random drives
 * the reader; the output is then order-dependent and not reproducible.
 */
public final class DataGenRowGenerator {

    private static final long GOLDEN = 0x9E3779B97F4A7C15L;
    private static final String WORD_ALPHABET = "abcdefghijklmnopqrstuvwxyz";

    private final List<ColumnRule> columns;
    private final FluxRow[] presetRows;
    private final Long seed;
    private final Random sharedRandom;

    public DataGenRowGenerator(DataGenSourceConfig config) {
        Objects.requireNonNull(config, "config must not be null");

        this.columns = config.getColumns();
        this.seed = config.getSeed();
        this.sharedRandom = new Random();
        this.presetRows = config.hasPresetRows() ? convertPresetRows(config) : null;
    }

    public int getArity() {
        return columns.size();
    }

    public FluxRow generate(long globalRow) {
        if (presetRows != null) {
            return presetRows[(int) globalRow];
        }

        Random random = seed == null
                ? sharedRandom
                : new Random(seed ^ (globalRow * GOLDEN));

        Object[] values = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            values[i] = generateValue(columns.get(i), globalRow, random);
        }
        return FluxRow.copyOf(values);
    }

    private Object generateValue(ColumnRule column, long globalRow, Random random) {
        switch (column.getSqlType()) {
            case STRING:
                return generateString(column, random);
            case BOOLEAN:
                return generateBoolean(column, random);
            case TINYINT:
                return (byte) generateIntegral(column, globalRow, random);
            case SMALLINT:
                return (short) generateIntegral(column, globalRow, random);
            case INT:
                return (int) generateIntegral(column, globalRow, random);
            case BIGINT:
                return generateIntegral(column, globalRow, random);
            case FLOAT:
                return (float) generateFraction(column, random);
            case DOUBLE:
                return generateFraction(column, random);
            case DECIMAL:
                return generateDecimal(column, random);
            case DATE:
                return generateDate(column, random);
            case TIME:
                return generateTime(column, random);
            case TIMESTAMP:
                return generateTimestamp(column, random);
            default:
                throw new IllegalStateException(
                        "DataGen column '" + column.getName() + "' has an unsupported type: "
                                + column.getSqlType());
        }
    }

    private Object generateString(ColumnRule column, Random random) {
        if (column.getMode() == ColumnRule.GeneratorMode.PICK) {
            return pick(column, random);
        }
        return randomWord(random, column.getStringLength());
    }

    private Object generateBoolean(ColumnRule column, Random random) {
        if (column.getMode() == ColumnRule.GeneratorMode.PICK) {
            return pick(column, random);
        }
        return random.nextBoolean();
    }

    private long generateIntegral(ColumnRule column, long globalRow, Random random) {
        if (column.getMode() == ColumnRule.GeneratorMode.PICK) {
            return (Long) pick(column, random);
        }
        if (column.getMode() == ColumnRule.GeneratorMode.SEQUENCE) {
            return column.getSequenceStart() + globalRow;
        }
        return nextLongBetween(random, column.getLongMin(), column.getLongMax());
    }

    private double generateFraction(ColumnRule column, Random random) {
        if (column.getMode() == ColumnRule.GeneratorMode.PICK) {
            return (Double) pick(column, random);
        }
        double min = column.getDoubleMin();
        double max = column.getDoubleMax();
        if (min == max) {
            return min;
        }
        return min + random.nextDouble() * (max - min);
    }

    private BigDecimal generateDecimal(ColumnRule column, Random random) {
        BigDecimal min = column.getDecimalMin();
        BigDecimal max = column.getDecimalMax();
        int scale = column.getScale();

        long minUnscaled = min.setScale(scale).unscaledValue().longValueExact();
        long maxUnscaled = max.setScale(scale).unscaledValue().longValueExact();
        long drawn = nextLongBetween(random, minUnscaled, maxUnscaled);
        return BigDecimal.valueOf(drawn, scale);
    }

    private Object generateDate(ColumnRule column, Random random) {
        if (column.getMode() == ColumnRule.GeneratorMode.PICK) {
            return pick(column, random);
        }
        return LocalDate.of(2024, 1, 1).plusDays(nextLongBetween(random, 0L, 3649L));
    }

    private Object generateTime(ColumnRule column, Random random) {
        if (column.getMode() == ColumnRule.GeneratorMode.PICK) {
            return pick(column, random);
        }
        return LocalTime.of(
                random.nextInt(24),
                random.nextInt(60),
                random.nextInt(60));
    }

    private Object generateTimestamp(ColumnRule column, Random random) {
        if (column.getMode() == ColumnRule.GeneratorMode.PICK) {
            return pick(column, random);
        }
        return LocalDateTime.of(2024, 1, 1, 0, 0, 0)
                .plusSeconds(nextLongBetween(random, 0L, 3650L * 24L * 60L * 60L - 1L));
    }

    private Object pick(ColumnRule column, Random random) {
        List<Object> values = column.getPickValues();
        return values.get(random.nextInt(values.size()));
    }

    private FluxRow[] convertPresetRows(DataGenSourceConfig config) {
        List<ColumnRule> rules = config.getColumns();
        List<List<Object>> rows = config.getPresetRows();

        FluxRow[] converted = new FluxRow[rows.size()];
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            List<Object> rawRow = rows.get(rowIndex);
            Object[] values = new Object[rules.size()];
            for (int columnIndex = 0; columnIndex < rules.size(); columnIndex++) {
                values[columnIndex] = rules.get(columnIndex).convertPresetCell(
                        columnIndex,
                        rawRow.get(columnIndex));
            }
            converted[rowIndex] = FluxRow.copyOf(values);
        }
        return converted;
    }

    private static String randomWord(Random random, int length) {
        StringBuilder word = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            word.append(WORD_ALPHABET.charAt(random.nextInt(WORD_ALPHABET.length())));
        }
        return word.toString();
    }

    /**
     * Uniform-enough bounded draw via unsigned remainder; the modulo bias is
     * negligible for generator ranges and tests only assert bounds.
     */
    private static long nextLongBetween(Random random, long min, long max) {
        if (min == max) {
            return min;
        }
        long range = max - min + 1L;
        return min + Long.remainderUnsigned(random.nextLong(), range);
    }
}
