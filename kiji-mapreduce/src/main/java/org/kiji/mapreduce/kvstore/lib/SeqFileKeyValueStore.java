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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ReflectionUtils;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;

/**
 * KeyValueStore implementation that reads records from SequenceFiles.
 *
 * <p>When specifying a SeqFileKeyValueStore in a kvstores XML file, you may
 * use the properties listed in {@link FileKeyValueStore}. No further properties
 * are provided for this implementation.</p>
 *
 * @param <K> The type of the key field stored in the SequenceFile(s).
 * @param <V> The type of value field stored in the SequenceFile(s).
 */
@ApiAudience.Public
public class SeqFileKeyValueStore<K, V> extends FileKeyValueStore<K, V> {

  /** Class that represents the options available to configure a SeqFileKeyValueStore. */
  public static class Options extends FileKeyValueStore.Options<Options> {
    // This is currently empty; placeholder for future SeqFileKeyValueStore-specific
    // options. Right now, everything we need can be handled by the FileKeyValueStore.Options.

    /** Default constructor. */
    public Options() {
    }
  }

  /** Default constructor. Used only for reflection. */
  public SeqFileKeyValueStore() {
    this(new Options());
  }

  /**
   * Main constructor; create a new SeqFileKeyValueStore to read SequenceFiles.
   *
   * @param options the options that configure the file store.
   */
  public SeqFileKeyValueStore(Options options) {
    super(options);
  }

  /** {@inheritDoc} */
  @Override
  public KeyValueStoreReader<K, V> open() throws IOException {
    return new Reader(getConf(), getExpandedInputPaths());
  }

  /**
   * Reads an entire SequenceFile of records into memory, and indexes it by the key field.
   *
   * <p>Lookups for a key <i>K</i> will return the first record in the file where the key field
   * has value <i>K</i>.</p>
   */
  private class Reader implements KeyValueStoreReader<K, V> {
    /** A map from key field to its corresponding value in the SequenceFile. */
    private Map<K, V> mMap;

    /**
     * Constructs a key value reader over a SequenceFile.
     *
     * @param conf The Hadoop configuration.
     * @param paths The path to the Avro file(s).
     * @throws IOException If the seqfile cannot be read.
     */
    @SuppressWarnings("unchecked")
    public Reader(Configuration conf, List<Path> paths) throws IOException {
      mMap = new TreeMap<K, V>();

      for (Path path : paths) {
        // Load the entire SequenceFile into the lookup map.
        FileSystem fs = path.getFileSystem(conf);
        SequenceFile.Reader seqReader = new SequenceFile.Reader(fs, path, conf);
        try {
          Class<? extends K> keyClass = (Class<? extends K>) seqReader.getKeyClass();
          Class<? extends V> valClass = (Class<? extends V>) seqReader.getValueClass();
          K key = ReflectionUtils.newInstance(keyClass, conf);
          V val = ReflectionUtils.newInstance(valClass, conf);

          key = (K) seqReader.next(key);
          while (key != null) {
            val = (V) seqReader.getCurrentValue(val);
            if (!mMap.containsKey(key)) {
              mMap.put(key, val);
            }

            // Get new instances of key and val to populate.
            key = ReflectionUtils.newInstance(keyClass, conf);
            val = ReflectionUtils.newInstance(valClass, conf);

            // Load the next key; returns null if we're out of file.
            key = (K) seqReader.next(key);
          }
        } finally {
          seqReader.close();
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
    public V get(K key) throws IOException {
      if (!isOpen()) {
        throw new IOException("Reader is closed");
      }
      return mMap.get(key);
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsKey(K key) throws IOException {
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
