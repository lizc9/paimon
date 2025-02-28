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

package org.apache.paimon.flink.action.cdc.format;

import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.kafka.KafkaDebeziumJsonDeserializationSchema;
import org.apache.paimon.flink.action.cdc.serialization.CdcJsonPulsarDeserializationSchema;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import java.util.function.Function;

/**
 * The message queue's record json deserialization class common implementation of {@link
 * DataFormat}.
 */
public abstract class AbstractJsonDataFormat extends AbstractDataFormat {

    @Override
    protected Function<Configuration, KafkaDeserializationSchema<CdcSourceRecord>>
            kafkaDeserializer() {
        return KafkaDebeziumJsonDeserializationSchema::new;
    }

    @Override
    protected Function<Configuration, PulsarDeserializationSchema<CdcSourceRecord>>
            pulsarDeserializer() {
        return CdcJsonPulsarDeserializationSchema::new;
    }
}
