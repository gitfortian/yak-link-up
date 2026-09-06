# Link-Up isolated connector plugins

The standalone distribution keeps mutually incompatible connector SDKs outside `lib/`.

Each immediate subdirectory under `plugins/` is loaded through its own `ConnectorClassLoader`:

```text
plugins/
  elasticsearch7/
    link-up-connector-elasticsearch7-*.jar
    elasticsearch-rest-high-level-client-7.*.jar
    elasticsearch-rest-client-7.*.jar
    ...

  elasticsearch8/
    link-up-connector-elasticsearch8-*.jar
    elasticsearch-java-8.*.jar
    elasticsearch-rest-client-8.*.jar
    ...
```

`bin/link-up-server.sh` discovers immediate plugin subdirectories automatically. Override the root with:

```bash
export LINK_UP_PLUGIN_ROOT=/opt/link-up/plugins
```

Or provide an explicit comma-separated directory list:

```bash
export LINK_UP_PLUGIN_DIRS=/opt/plugins/elasticsearch7,/opt/plugins/elasticsearch8
```

Do **not** merge the Elasticsearch 7 and Elasticsearch 8 jars into one plugin directory and do not add them to `lib/`. The two connector families intentionally depend on incompatible versions of `org.elasticsearch.client:elasticsearch-rest-client`.

The Worker connector inventory endpoints remain the runtime source of truth. A control plane should enable Elasticsearch execution only when `elasticsearch7` / `elasticsearch8` SOURCE and SINK schemas are actually reported by the configured Worker.
