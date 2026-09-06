package com.link.up.connector.mongodb.config;

import com.link.up.api.configuration.Option;
import com.link.up.api.configuration.Options;
import com.link.up.api.connector.schema.ConnectorOptionScope;

/** User-facing options for the bounded MongoDB Sink. */
public final class MongoSinkOptions {

    private MongoSinkOptions() {
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
                    .withDescription("One MongoDB target collection")
                    .withSemanticType("COLLECTION")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<String> DOCUMENT_ID_FIELD =
            Options.key("document_id_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Optional source field copied to MongoDB _id")
                    .withSemanticType("DOCUMENT_ID_FIELD")
                    .withScope(ConnectorOptionScope.TASK);

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Documents per ordered MongoDB insertMany flush")
                    .withSemanticType("BATCH_ROWS")
                    .withScope(ConnectorOptionScope.RUNTIME);
}
