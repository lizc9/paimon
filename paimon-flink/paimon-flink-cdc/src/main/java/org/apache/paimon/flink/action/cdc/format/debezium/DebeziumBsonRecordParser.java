/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.action.cdc.format.debezium;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.action.cdc.format.AbstractJsonRecordParser;
import org.apache.paimon.flink.action.cdc.mongodb.BsonValueConvertor;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.TypeUtils;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.FIELD_AFTER;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.FIELD_BEFORE;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.FIELD_DB;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.FIELD_PAYLOAD;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.FIELD_SCHEMA;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.FIELD_SOURCE;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.FIELD_TYPE;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.OP_DELETE;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.OP_INSERT;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.OP_READE;
import static org.apache.paimon.flink.action.cdc.format.debezium.DebeziumSchemaUtils.OP_UPDATE;
import static org.apache.paimon.utils.JsonSerdeUtil.fromJson;
import static org.apache.paimon.utils.JsonSerdeUtil.isNull;
import static org.apache.paimon.utils.JsonSerdeUtil.writeValueAsString;

/**
 * The {@code DebeziumBsonRecordParser} class extends the abstract {@link AbstractJsonRecordParser} and
 * is designed to parse records from Debezium's BSON change data capture (CDC) format. Debezium is a
 * CDC solution for Mongodb databases that captures row-level changes to database tables and outputs
 * them in BSON format. This parser extracts relevant information from the Debezium-BSON format and
 * converts it into a list of {@link RichCdcMultiplexRecord} objects.
 *
 * <p>The class supports various database operations such as INSERT, UPDATE, DELETE, and READ
 * (snapshot reads), and creates corresponding {@link RichCdcMultiplexRecord} objects to represent
 * these changes.
 *
 * <p>Validation is performed to ensure that the JSON records contain all necessary fields,
 * including the 'before' and 'after' states for UPDATE operations, and the class also supports
 * schema extraction for the Kafka topic. Debezium's specific fields such as 'source', 'op' for
 * operation type, and primary key field names are used to construct the details of each record
 * event.
 */
public class DebeziumBsonRecordParser extends AbstractJsonRecordParser {

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumBsonRecordParser.class);

    protected static final String FIELD_COLLECTION = "collection";
    protected static final String FIELD_OBJECT_ID = "_id";
    protected static final List<String> PRIMARY_KEYS = Collections.singletonList(FIELD_OBJECT_ID);

    public DebeziumBsonRecordParser(TypeMapping typeMapping, List<ComputedColumn> computedColumns) {
        super(typeMapping, computedColumns);
    }

    @Override
    public List<RichCdcMultiplexRecord> extractRecords() {
        String operation = getAndCheck(FIELD_TYPE).asText();
        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        switch (operation) {
            case OP_INSERT:
            case OP_READE:
                processRecord(getData(), RowKind.INSERT, records);
                break;
            case OP_UPDATE:
                processRecord(
                        mergeOldRecord(getData(), getBefore(operation)), RowKind.DELETE, records);
                processRecord(getData(), RowKind.INSERT, records);
                break;
            case OP_DELETE:
                processRecord(getBefore(operation), RowKind.DELETE, records);
                break;
            default:
                throw new UnsupportedOperationException("Unknown record operation: " + operation);
        }
        return records;
    }

    private JsonNode getData() {
        return getAndCheck(dataField());
    }

    private JsonNode getBefore(String op) {
        return getAndCheck(FIELD_BEFORE, FIELD_TYPE, op);
    }

    @Override
    protected void setRoot(CdcSourceRecord record) {
        JsonNode node = (JsonNode) record.getValue();
        if (node.has(FIELD_SCHEMA)) {
            root = node.get(FIELD_PAYLOAD);
        } else {
            root = node;
        }
    }

    @Override
    protected Map<String, String> extractRowData(JsonNode record, RowType.Builder rowTypeBuilder) {
        // bson record should be a string
        if (!record.isTextual()) {
            return super.extractRowData(record, rowTypeBuilder);
        }

        BsonDocument document = BsonDocument.parse(record.asText());
        LinkedHashMap<String, String> resultMap = new LinkedHashMap<>();
        for (Map.Entry<String, BsonValue> entry : document.entrySet()) {
            String fieldName = entry.getKey();
            resultMap.put(fieldName, toJsonString(BsonValueConvertor.convert(entry.getValue())));
            rowTypeBuilder.field(fieldName, DataTypes.STRING());
        }

        evalComputedColumns(resultMap, rowTypeBuilder);

        return resultMap;
    }

    private static String toJsonString(Object entry) {
        if (entry == null) {
            return null;
        } else if (!TypeUtils.isBasicType(entry)) {
            try {
                return writeValueAsString(entry);
            } catch (JsonProcessingException e) {
                LOG.error("Failed to deserialize record.", e);
            }
        }
        return Objects.toString(entry);
    }

    @Override
    protected List<String> extractPrimaryKeys() {
        return PRIMARY_KEYS;
    }

    @Override
    protected String primaryField() {
        return FIELD_OBJECT_ID;
    }

    @Override
    protected String dataField() {
        return FIELD_AFTER;
    }

    @Nullable
    @Override
    protected String getTableName() {
        return getFromSourceField(FIELD_COLLECTION);
    }

    @Nullable
    @Override
    protected String getDatabaseName() {
        return getFromSourceField(FIELD_DB);
    }

    @Override
    protected String format() {
        return "debezium-bson";
    }

    @Nullable
    private String getFromSourceField(String key) {
        JsonNode node = root.get(FIELD_SOURCE);
        if (isNull(node)) {
            return null;
        }

        node = node.get(key);
        return isNull(node) ? null : node.asText();
    }
}
