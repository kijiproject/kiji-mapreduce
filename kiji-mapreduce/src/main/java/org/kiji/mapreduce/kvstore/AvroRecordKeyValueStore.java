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
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.hadoop.util.AvroCharSequenceComparator;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.KeyValueStoreConfiguration;
import org.kiji.mapreduce.KeyValueStoreReader;

/**
 * An interface for providing read access to Avro container files of records.
 *
 * <p>This KeyValueStore provides lookup access to an Avro container file by reading the
 * entire file into memory and indexing the records by the <i>key field</i>.  The <i>key
 * field</i> must be specified when constructing the store. The value returned from a
 * lookup of key <i>K</i> will be the first record whose key field has the value
 * <i>K</i>.</p>
 *
 * <p>In addition to the properties listed in {@link FileKeyValueStore}, a kvstores
 * XML file may contain the following properties when specifying the behavior of this
 * class:</p>
 * <ul>
 *   <li><tt>avro.reader.schema</tt> - The reader schema to apply to records in the
 *       input file(s).</li>
 *   <li><tt>key.field</tt> - The name of the field of the input records to treat as
 *       the key in the KeyValueStore.</li>
 * </ul>
 *
 * @param <K> The type of the key field.
 * @param <V> The type of record in the Avro container file.
 */
@ApiAudience.Public
public class AvroRecordKeyValueStore<K, V extends IndexedRecord>
    extends FileKeyValueStore<K, V> {

  /** The configuration variable for the Avro record reader schema. */
  private static final String CONF_READER_SCHEMA_KEY = "avro.reader.schema";

  /** The configuration variable for the name of the field to use as the lookup key. */
  private static final String CONF_KEY_FIELD_KEY = "key.field";

  /** The schema to use for reading the Avro records. */
  private Schema mReaderSchema;

  /** The name of the field to use as the lookup key for records. */
  private String mKeyFieldName;

  /**
   * An object to encapsulate the numerous options of an AvroRecordKeyValueStore.
   */
  public static class Options extends FileKeyValueStore.Options<Options> {
    private Schema mReaderSchema;
    private String mKeyFieldName;

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
     * Sets the name of the record field to use as the lookup key.
     *
     * @param keyFieldName The name of the key field.
     * @return This options instance.
     */
    public Options withKeyFieldName(String keyFieldName) {
      mKeyFieldName = keyFieldName;
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

    /**
     * Gets the name of the field to use as the lookup key.
     *
     * @return The key field name.
     */
    public String getKeyFieldName() {
      return mKeyFieldName;
    }
  }


  /**
   * Constructs an AvroRecordKeyValueStore.
   *
   * @param options The options for configuring the store.
   */
  public AvroRecordKeyValueStore(Options options) {
    super(options);
    setConf(options.getConfiguration());
    setReaderSchema(options.getReaderSchema());
    setKeyFieldName(options.getKeyFieldName());
  }

  /**
   * Constructs an unconfigured AvroRecordKeyValueStore.
   *
   * <p>Do not use this constructor. It is for instantiation via ReflectionUtils.newInstance().</p>
   */
  public AvroRecordKeyValueStore() {
    this(new Options());
  }

  /**
   * Sets the reader schema to use for the avro container file records.
   *
   * @param readerSchema The reader schema.
   */
  public void setReaderSchema(Schema readerSchema) {
    mReaderSchema = readerSchema;
  }

  /**
   * Sets the field to use as the lookup key for the records.
   *
   * @param keyFieldName The key field.
   */
  public void setKeyFieldName(String keyFieldName) {
    mKeyFieldName = keyFieldName;
  }

  /** {@inheritDoc} */
  @Override
  public void storeToConf(KeyValueStoreConfiguration conf) throws IOException {
    storeToConf(conf, true);
  }

  /**
   * Implements storeToConf() but optionally suppresses storing the actual
   * FileSystem state associated with this KeyValueStore. This is used by the
   * AvroKVRecordKeyValueStore when wrapping an instance of this KeyValueStore;
   * the wrapper manages the file data, whereas this KeyValueStore persists only
   * reader schema, key field, etc.
   *
   * @param conf the KeyValueStoreConfiguration to persist to.
   * @param persistFsState true if the filenames, etc. associated with this store should
   *     be written to the Configuration, false if this is externally managed.
   * @throws IOException if there is an error communicating with the FileSystem.
   */
  void storeToConf(KeyValueStoreConfiguration conf, boolean persistFsState)
      throws IOException {

    if (persistFsState) {
      super.storeToConf(conf);
    }

    if (null != mReaderSchema) {
      conf.set(CONF_READER_SCHEMA_KEY, mReaderSchema.toString());
    }

    if (null == mKeyFieldName) {
      throw new IOException("Required attribute not set: keyField");
    }
    conf.set(CONF_KEY_FIELD_KEY, mKeyFieldName);
  }

  /** {@inheritDoc} */
  @Override
  public void initFromConf(KeyValueStoreConfiguration conf) throws IOException {
    super.initFromConf(conf);

    String schema = conf.get(CONF_READER_SCHEMA_KEY);
    if (null != schema) {
      setReaderSchema(Schema.parse(schema));
    }

    setKeyFieldName(conf.get(CONF_KEY_FIELD_KEY));
  }

  /** {@inheritDoc} */
  @Override
  public KeyValueStoreReader<K, V> open() throws IOException, InterruptedException {
    return new Reader<K, V>(getConf(), getExpandedInputPaths(), mReaderSchema, mKeyFieldName);
  }

  /**
   * Reads an entire Avro container file of records into memory, and indexes it by the key field.
   *
   * <p>Lookups for a key <i>K</i> will return the first record in the file where the key field
   * has value <i>K</i>.</p>
   */
  static class Reader<K, V extends IndexedRecord> extends KeyValueStoreReader<K, V> {
    /** A map from key field to its corresponding record in the Avro container file. */
    private Map<K, V> mMap;

    /**
     * Constructs a key value reader over an Avro file.
     *
     * @param conf The Hadoop configuration.
     * @param paths The path to the Avro file(s).
     * @param readerSchema The reader schema for the records within the Avro file.
     * @param keyFieldName The name of the field within the record to use as the lookup key.
     * @throws IOException If the Avro file cannot be read, or the key field is not found.
     */
    public Reader(Configuration conf, List<Path> paths, Schema readerSchema, String keyFieldName)
        throws IOException {
      for (Path path : paths) {
        // Load the entire Avro file into the lookup map.
        DataFileStream<V> avroFileReader = null;
        try {
          avroFileReader = new DataFileStream<V>(path.getFileSystem(conf).open(path),
              new SpecificDatumReader<V>(readerSchema));
          if (null == readerSchema) {
            // If the user has not specified a reader schema, grab the schema from the
            // first file we encounter.
            readerSchema = avroFileReader.getSchema();
          }

          // Check that the key field exists in the reader schema.
          Schema.Field avroKeyField = readerSchema.getField(keyFieldName);
          if (null == avroKeyField) {
            throw new IOException("Key field " + keyFieldName
                + " was not found in the record schema: " + readerSchema.toString());
          }

          if (null == mMap) {
            // Set up the in-memory lookup map for Avro records.
            if (avroKeyField.schema().equals(Schema.create(Schema.Type.STRING))) {
              // Special case Avro string comparison, since we want to allow comparison of
              // String objects with Utf8 objects.
              mMap = new TreeMap<K, V>(new AvroCharSequenceComparator<K>());
            } else {
              mMap = new TreeMap<K, V>();
            }
          }

          for (V record : avroFileReader) {
            // Here, we read the value of the key field by looking it up by its record
            // position (index).  In other words, to get the value of the 'key' field, we ask
            // the Schema what position the 'key' field has, and use that position to get the
            // value from the IndexedRecord.
            //
            // We have to do this because there is no common interface between GenericRecord
            // and SpecificRecord that allows us to read fields by name.  The unit test seems
            // to suggest that this approach works just fine, but if we find a bug later,
            // the safe fix is to sacrifice the ability to read SpecificRecords -- all
            // values of this store could just be GenericData.Record instances read by the
            // GenericDatumReader.
            @SuppressWarnings("unchecked")
            K key = (K) record.get(avroKeyField.pos());
            if (!mMap.containsKey(key)) {
              mMap.put(key, record);
            }
          }
        } finally {
          if (null != avroFileReader) {
            avroFileReader.close();
          }
        }
      }
    }

    /** {@inheritDoc} */
    @Override
    public boolean isOpen() {
      return null != mMap;
    }

    /** {@inheritDoc} */
    @Override
    public V get(K key) throws IOException, InterruptedException {
      if (!isOpen()) {
        throw new IOException("Reader is closed");
      }
      return mMap.get(key);
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsKey(K key) throws IOException, InterruptedException {
      if (!isOpen()) {
        throw new IOException("Reader is closed");
      }
      return mMap.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      mMap = null;
    }
  }
}
