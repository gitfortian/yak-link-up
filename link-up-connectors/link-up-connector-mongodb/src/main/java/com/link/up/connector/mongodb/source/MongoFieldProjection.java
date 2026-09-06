package com.link.up.connector.mongodb.source;

import org.bson.BsonDocument;
import org.bson.BsonInt32;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Builds an inclusion projection without MongoDB parent/child path collisions. */
final class MongoFieldProjection {

    private MongoFieldProjection() {
    }

    static BsonDocument build(List<String> fields) {
        if (fields == null || fields.isEmpty()) {
            return new BsonDocument();
        }

        Set<String> selected = new LinkedHashSet<String>(fields);
        BsonDocument projection = new BsonDocument();
        for (String field : selected) {
            if (!hasSelectedAncestor(field, selected)) {
                projection.put(field, new BsonInt32(1));
            }
        }

        if (!selectsId(selected)) {
            projection.put("_id", new BsonInt32(0));
        }
        return projection;
    }

    private static boolean hasSelectedAncestor(String field, Set<String> selected) {
        int separator = field.lastIndexOf('.');
        while (separator > 0) {
            String ancestor = field.substring(0, separator);
            if (selected.contains(ancestor)) {
                return true;
            }
            separator = ancestor.lastIndexOf('.');
        }
        return false;
    }

    private static boolean selectsId(Set<String> selected) {
        for (String field : selected) {
            if ("_id".equals(field) || field.startsWith("_id.")) {
                return true;
            }
        }
        return false;
    }
}
