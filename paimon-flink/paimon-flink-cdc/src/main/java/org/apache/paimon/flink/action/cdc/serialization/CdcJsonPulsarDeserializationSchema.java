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

package org.apache.paimon.flink.action.cdc.serialization;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.pulsar.client.api.Message;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

/** A simple deserialization schema for {@link CdcSourceRecord}. */
public class CdcJsonPulsarDeserializationSchema
        implements PulsarDeserializationSchema<CdcSourceRecord> {

    private static final long serialVersionUID = 1L;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public CdcJsonPulsarDeserializationSchema(Configuration cdcSourceConfig) {
        this();
    }

    public CdcJsonPulsarDeserializationSchema() {
        objectMapper
                .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public void deserialize(Message<byte[]> message, Collector<CdcSourceRecord> collector)
            throws Exception {
        byte[] value = message.getData();
        if (value == null) {
            // tombstone message
            return;
        }

        JsonNode keyNode = null;
        byte[] key = message.getKeyBytes();
        JsonNode valueNode = objectMapper.readValue(value, JsonNode.class);
        if (key != null) {
            keyNode = objectMapper.readValue(key, JsonNode.class);
        }
        collector.collect(new CdcSourceRecord(null, keyNode, valueNode));
    }

    @Override
    public TypeInformation<CdcSourceRecord> getProducedType() {
        return getForClass(CdcSourceRecord.class);
    }
}
