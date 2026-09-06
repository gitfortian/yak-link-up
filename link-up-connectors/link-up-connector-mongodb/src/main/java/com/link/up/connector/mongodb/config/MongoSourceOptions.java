package com.link.up.connector.mongodb.config;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.Options;
import com.link.up.api.connector.schema.ConnectorOptionScope;

import java.util.List;

/** User-facing options for the bounded MongoDB Source. */
public final class MongoSourceOptions {

    private MongoSourceOptions() {
    }

    public static final Option<String> URI =
            Options.key("uri")
                    .stringType()
                    .noDefaultValue()
                    .sensitive()
                    .withDescription("MongoDB connection URI")
                    .withSemanticType("MONGODB_URI")
                    .withScope(ConnectorOptionScope.DATASOURCE);

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("MongoDB database; optional when the URI already contains one")
                    .withSemanticType("DATABASE")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> COLLECTION =
            Options.key("collection")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("One MongoDB collection")
                    .withSemanticType("COLLECTION")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<List<String>> FIELDS =
            Options.key("fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Optional field projection; dotted nested paths are supported")
                    .withSemanticType("SOURCE_FIELDS")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> FILTER =
            Options.key("filter")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Optional MongoDB find filter as Extended JSON")
                    .withSemanticType("QUERY")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Integer> FETCH_SIZE =
            Options.key("fetch_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Documents requested from MongoDB per cursor batch")
                    .withSemanticType("BATCH_ROWS")
                    .withScope(ConnectorOptionScope.RUNTIME);
}
