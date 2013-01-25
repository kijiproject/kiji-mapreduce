/**
 * (c) Copyright 2012 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.mapreduce.kvstore.lib;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import org.kiji.mapreduce.kvstore.KeyValueStoreConfiguration;
import org.kiji.mapreduce.kvstore.impl.KeyValueStoreConfigSerializer;

public class TestKijiTableKeyValueStore {
  /** @return an uninitialized store to test for initialization from a Configuration. */
  private <T> KijiTableKeyValueStore<T> getUninitializedStore() {
    return (KijiTableKeyValueStore<T>) ReflectionUtils.newInstance(
        KijiTableKeyValueStore.class, new Configuration());
  }

  @Test
  public void testSerialization() throws IOException {
    // Test that we can serialize a KijiTableKeyValueStore to a conf and resurrect it.
    KijiTableKeyValueStore<String> input = KijiTableKeyValueStore.builder()
        .withTable("tbl")
        .withColumn("some", "column")
        .withMinTimestamp(42)
        .withMaxTimestamp(512)
        .withCacheLimit(2121)
        .withReaderSchema(Schema.create(Schema.Type.STRING))
        .build();

    KeyValueStoreConfiguration conf = KeyValueStoreConfiguration.fromConf(new Configuration());

    input.storeToConf(conf);
    conf.getDelegate().set(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE
        + ".0." + KeyValueStoreConfigSerializer.CONF_NAME, "the-store-name");

    KijiTableKeyValueStore<String> output = getUninitializedStore();
    output.initFromConf(conf);

    assertEquals(input, output);
  }

  @Test
  public void testOkWithoutSchema() throws IOException {
    // Serializing without an explicit reader schema is ok.
    KijiTableKeyValueStore<String> input = KijiTableKeyValueStore.builder()
        .withTable("tbl")
        .withColumn("some", "column")
        .withMinTimestamp(42)
        .withMaxTimestamp(512)
        .withCacheLimit(2121)
        .build();

    KeyValueStoreConfiguration conf = KeyValueStoreConfiguration.fromConf(
        new Configuration(false));

    input.storeToConf(conf);
    conf.getDelegate().set(KeyValueStoreConfiguration.KEY_VALUE_STORE_NAMESPACE
        + ".0." + KeyValueStoreConfigSerializer.CONF_NAME, "the-store-name");

    KijiTableKeyValueStore<String> output = getUninitializedStore();
    output.initFromConf(conf);

    assertEquals(input, output);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testRequiresTable() {
    // Test that we need to set the table name, or it will fail to verify as input.
    KijiTableKeyValueStore<String> input = KijiTableKeyValueStore.builder()
        .withColumn("some", "column")
        .withMinTimestamp(42)
        .withMaxTimestamp(512)
        .withCacheLimit(2121)
        .build();
  }

  @Test(expected=IllegalArgumentException.class)
  public void testRequiresColumn() {
    // Test that we need to set the column to read.
    KijiTableKeyValueStore<String> input = KijiTableKeyValueStore.builder()
        .withTable("foo")
        .withMinTimestamp(42)
        .withMaxTimestamp(512)
        .withCacheLimit(2121)
        .build();
  }
}
