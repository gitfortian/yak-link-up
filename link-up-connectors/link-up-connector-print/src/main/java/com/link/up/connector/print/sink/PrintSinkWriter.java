package com.link.up.connector.print.sink;

import com.link.up.api.sink.SinkWriter;
import com.link.up.api.source.RecordBatch;
import com.link.up.api.table.catalog.CatalogTable;
import com.link.up.api.table.type.FluxRow;
import com.link.up.connector.print.config.PrintSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Writes every row as one INFO log line; nothing durable is ever produced.
 *
 * <p>The emitter is injectable so tests can assert on formatted lines without
 * a log backend. Row numbers count per writer from 1; parallel writers make no
 * cross-writer ordering promise, so consumers must compare row sets, not order.
 */
public final class PrintSinkWriter implements SinkWriter<FluxRow> {

    private static final Logger LOG = LoggerFactory.getLogger(PrintSinkWriter.class);

    interface LineEmitter {
        void emit(String line);
    }

    private final PrintSinkConfig config;
    private final LineEmitter emitter;
    private final PrintRowFormatter formatter = new PrintRowFormatter();
    private final Set<String> schemaLoggedDatasets = new LinkedHashSet<String>();

    private long rowNumber;
    private boolean closed;

    public PrintSinkWriter(PrintSinkConfig config) {
        this(config, new LineEmitter() {
            @Override
            public void emit(String line) {
                LOG.info(line);
            }
        });
    }

    PrintSinkWriter(
            PrintSinkConfig config,
            LineEmitter emitter) {

        this.config = Objects.requireNonNull(config, "config must not be null");
        this.emitter = Objects.requireNonNull(emitter, "emitter must not be null");
    }

    @Override
    public void write(
            RecordBatch<FluxRow> batch,
            CatalogTable sourceTable) {

        if (closed) {
            throw new IllegalStateException("PrintSinkWriter has already been closed");
        }
        Objects.requireNonNull(sourceTable, "sourceTable must not be null");
        if (batch == null || batch.isEndOfInput() || batch.getRecords().isEmpty()) {
            return;
        }

        String dataSetId = batch.getDataSetId();
        if (schemaLoggedDatasets.add(dataSetId)) {
            emitter.emit(formatter.formatSchema(dataSetId, sourceTable));
        }

        if (config.isPrintRows()) {
            String splitId = batch.getSplitId();
            for (FluxRow row : batch.getRecords()) {
                emitter.emit(formatter.formatRow(
                        dataSetId,
                        splitId,
                        ++rowNumber,
                        sourceTable,
                        row));
            }
        }

        sleepInterval();
    }

    @Override
    public void close() {
        closed = true;
    }

    private void sleepInterval() {
        if (config.getPrintIntervalMillis() <= 0) {
            return;
        }
        try {
            Thread.sleep(config.getPrintIntervalMillis());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Print write interrupted while waiting for the next batch",
                    interrupted);
        }
    }
}
