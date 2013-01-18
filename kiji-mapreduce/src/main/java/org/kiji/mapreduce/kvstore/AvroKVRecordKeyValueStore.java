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

package org.kiji.mapreduce.kvstore;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyValue;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.KeyValueStoreConfiguration;
import org.kiji.mapreduce.KeyValueStoreReader;

/**
 * An interface for providing read access to Avro container files of (key, value)
 * records.
 *
 * <p>This KeyValueStore provides lookup access to an Avro container file by reading
 * the entire file into memory. The Avro file is assumed to contain records with (at
 * least) two fields, "key" and "value." This store will decompose the top-level
 * record into its two fields, and index the "value" field by the key.</p>
 *
 * <p>In addition to the properties listed in {@link FileKeyValueStore}, a kvstores
 * XML file may contain the following properties when specifying the behavior of this
 * class:</p>
 * <ul>
 *   <li><tt>avro.reader.schema</tt> - The reader schema to apply to records in the
 *       input file(s).</li>
 * </ul>
 *
 * @param <K> The type of the key field.
 * @param <V> The type of the value field.
 */
@ApiAudience.Public
public class AvroKVRecordKeyValueStore<K, V> extends FileKeyValueStore<K, V> {
  /** A wrapped store for looking up an Avro record by its 'key' field. */
  private final AvroRecordKeyValueStore<K, GenericRecord> mStore;

  /**
   * An object to encapsulate the numerous options of an AvroKVRecordKeyValueStore.
   */
  public static class Options extends FileKeyValueStore.Options<Options> {
    private Schema mReaderSchema;

    /**
     * Sets the schema to read the records with.
     *
     * @param schema The reader schema.
     * @return This options instance.
     */
    public Options withReaderSchema(Schema schema) {
      mReaderSchema = schema;
      return this;
    }

    /**
     * Gets the schema used to read the records.
     *
     * @return The Avro reader schema.
     */
    public Schema getReaderSchema() {
      return mReaderSchema;
    }
  }

  /**
   * Constructs an AvroKVRecordKeyValueStore.
   *
   * @param options The options for configuring the store.
   */
  public AvroKVRecordKeyValueStore(Options options) {
    super(options);
    mStore = new AvroRecordKeyValueStore<K, GenericRecord>(new AvroRecordKeyValueStore.Options()
        .withConfiguration(options.getConfiguration())
        .withInputPaths(options.getInputPaths())
        .withDistributedCache(options.getUseDistributedCache())
        .withReaderSchema(options.getReaderSchema())
        .withKeyFieldName(AvroKeyValue.KEY_FIELD));
  }

  /**
   * Constructs an unconfigured AvroKVRecordKeyValueStore.
   *
   * <p>Do not use this constructor. It is for instantiation via
   * ReflectionUtils.newInstance().</p>
   */
  public AvroKVRecordKeyValueStore() {
    this(new Options());
  }

  /**
   * Sets the reader schema to use for the avro container file records.
   *
   * @param readerSchema The reader schema.
   */
  public void setReaderSchema(Schema readerSchema) {
    mStore.setReaderSchema(readerSchema);
  }

  /** {@inheritDoc} */
  @Override
  public void storeToConf(KeyValueStoreConfiguration conf) throws IOException {
    mStore.storeToConf(conf);
  }

  /** {@inheritDoc} */
  @Override
  public void initFromConf(KeyValueStoreConfiguration conf) throws IOException {
    mStore.initFromConf(conf);
  }

  /** {@inheritDoc} */
  @Override
  public KeyValueStoreReader<K, V> open() throws IOException, InterruptedException {
    // Delay mStore's input path configuration until here,
    // because setInputPaths(), etc. get called in our FileKeyValueStore's c'tor.
    // So make sure we use the right input paths that we were configured with.
    mStore.setInputPaths(getInputPaths());
    return new Reader<K, V>(mStore);
  }

  /**
   * Reads an entire Avro container file of (key, value) records into memory, indexed
   * by "key."
   *
   * <p>Lookups for a key <i>K</i> will return the "value" field of the first record
   * in the file where the key field has value <i>K</i>.</p>
   */
  static class Reader<K, V> extends KeyValueStoreReader<K, V> {
    /** A wrapped Avro store reader for looking up a record by its 'key' field. */
    private final KeyValueStoreReader<K, GenericRecord> mReader;

    /**
     * Constructs a key value reader over an Avro file.
     *
     * @param store An Avro file store that uses the 'key' field as the key, and
     *     the entire record as the value.
     * @throws IOException If there is an error.
     * @throws InterruptedException If the thread is interrupted.
     */
    public Reader(AvroRecordKeyValueStore<K, GenericRecord> store)
        throws IOException, InterruptedException {
      mReader = store.open();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isOpen() {
      return mReader.isOpen();
    }

    /** {@inheritDoc} */
    @Override
    @SuppressWarnings("unchecked")
    public V get(K key) throws IOException, InterruptedException {
      GenericRecord record = mReader.get(key);
      if (null == record) {
        // No match;
        return null;
      }

      return (V) record.get(AvroKeyValue.VALUE_FIELD);
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsKey(K key) throws IOException, InterruptedException {
      return mReader.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      mReader.close();
    }
  }
}
