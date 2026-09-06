package com.link.up.connector.doris.client.source.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Tablet routing returned by the Doris FE Query Plan endpoint. */
public final class DorisTablet implements Serializable {

    private static final long serialVersionUID = 1L;

    private List<String> routings = new ArrayList<String>();
    private long version;
    private long versionHash;
    private long schemaHash;

    public List<String> getRoutings() {
        return routings == null ? Collections.<String>emptyList() : routings;
    }
    public void setRoutings(List<String> routings) { this.routings = routings; }
    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }
    public long getVersionHash() { return versionHash; }
    public void setVersionHash(long versionHash) { this.versionHash = versionHash; }
    public long getSchemaHash() { return schemaHash; }
    public void setSchemaHash(long schemaHash) { this.schemaHash = schemaHash; }
}
