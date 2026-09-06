package com.link.up.connector.doris.converter;

import com.link.up.api.table.type.BasicType;
import com.link.up.api.table.type.FluxDataType;
import com.link.up.api.table.type.FluxRow;
import com.link.up.api.table.type.FluxRowType;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DorisArrowRowReaderTest {

    @Test
    public void decodesArrowIpcBatchByProjectedFieldName() throws Exception {
        byte[] payload;
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            BigIntVector id = new BigIntVector("id", allocator);
            VarCharVector name = new VarCharVector("name", allocator);
            id.allocateNew(2);
            name.allocateNew();
            id.setSafe(0, 7L);
            id.setSafe(1, 8L);
            name.setSafe(0, "alice".getBytes(StandardCharsets.UTF_8));
            name.setNull(1);
            id.setValueCount(2);
            name.setValueCount(2);

            try (VectorSchemaRoot root = VectorSchemaRoot.of(name, id);
                 ByteArrayOutputStream output = new ByteArrayOutputStream();
                 ArrowStreamWriter writer = new ArrowStreamWriter(root, null, output)) {
                root.setRowCount(2);
                writer.start();
                writer.writeBatch();
                writer.end();
                payload = output.toByteArray();
            }
        }

        FluxRowType rowType =
                new FluxRowType(
                        new String[] {"id", "name"},
                        new FluxDataType<?>[] {BasicType.LONG_TYPE, BasicType.STRING_TYPE});
        List<FluxRow> rows = DorisArrowRowReader.read(payload, rowType);

        assertEquals(2, rows.size());
        assertEquals(7L, rows.get(0).getField(0));
        assertEquals("alice", rows.get(0).getField(1));
        assertEquals(8L, rows.get(1).getField(0));
        assertEquals(null, rows.get(1).getField(1));
    }
}
