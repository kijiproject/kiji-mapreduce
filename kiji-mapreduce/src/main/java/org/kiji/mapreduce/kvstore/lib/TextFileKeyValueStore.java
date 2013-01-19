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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.kvstore.KeyValueStoreConfiguration;
import org.kiji.mapreduce.kvstore.KeyValueStoreReader;

/**
 * KeyValueStore implementation that reads delimited records from text files.
 *
 * <p>Each line of an input text file is made available as a record in the KeyValueStore.
 * Lines are separated into keys and values by the first instance of the <i>delimiter</i> string.
 * The second (and additional) instances of the delimiter string in the line are part of the
 * value, and are not treated specially.</p>
 *
 * <p>Input lines are read through BufferedReader.readLine(). A line is considered to be
 * terminated by any of a line feed ('\n'), carriage return ('\r'), or a carriage return
 * followed immediately by a linefeed.</p>
 *
 * <p>Line termination characters are removed before data is put in the KeyValueStore.</p>
 *
 * <p>By default, keys and values are tab-delimited. The delimiter may be set by the
 * <tt>setDelimiter()</tt> method. The delimiter may be multiple characters long.
 * The following rules are applied when breaking lines into keys and values:</p>
 * <ul>
 *   <li>Keys and values are separated by the first instance of the delimiter in the line.
 *       Further instances of the delimiter string are retained as part of the value.</li>
 *   <li>A key followed immediately by a line terminator has a <tt>null</tt> value associated
 *       with it. <tt>containsKey()</tt> will return <tt>true</tt> for this key, and
 *       <tt>get()</tt> will return <tt>null</tt>.</li>
 *   <li>A key followed by the delimiter and then the line terminator has the empty string
 *       (<tt>&quot;&quot;</tt>) as its value.</li>
 *   <li>A line that begins with a delimiter has the empty string as its key; the remainder
 *       of the line following the delimiter is the value.</li>
 *   <li><tt>null</tt> is never allowed or possible as a key.</li>
 *   <li>A blank line uses the above rules as follows: the key is the empty string
 *       (<tt>&quot;&quot;</tt>), and the value is <tt>null</tt>.</li>
 *   <li>The last line in the file does not need to be newline-terminated; this is treated
 *       like any other line in the file.</li>
 * </ul>
 *
 * <p>In addition to the properties listed in {@link FileKeyValueStore}, a kvstores
 * XML file may contain the following properties when specifying the behavior of this
 * class:</p>
 * <ul>
 *   <li><tt>delim</tt> - The delimiter string that separates keys and values within
 *       a line of text. (Default is a tab character.)</li>
 * </ul>
 *
 */
@ApiAudience.Public
public class TextFileKeyValueStore extends FileKeyValueStore<String, String> {

  /** The configuration variable for the delimiter. */
  private static final String CONF_DELIMITER_KEY = "delim";

  /**
   * Records are tab-delimited by default, to be compatible with TextOutputFormat,
   * and function like KeyValueTextInputFormat.
   */
  public static final String DEFAULT_DELIMITER = "\t";

  /** Class that represents the options available to configure a TextFileKeyValueStore. */
  public static class Options extends FileKeyValueStore.Options<Options> {
    private String mDelim;

    /** Default constructor. */
    public Options() {
      mDelim = DEFAULT_DELIMITER;
    }

    /**
     * Specifies the delimiter between the key and the value on a line in the file.
     *
     * @param delim the delimiter string to use.
     * @return this Options instance.
     */
    public Options withDelimiter(String delim) {
      mDelim = delim;
      return this;
    }

    /**
     * Returns the delimiter string. This defaults to a tab character.
     *
     * @return the delimiter string.
     */
    public String getDelimiter() {
      return mDelim;
    }
  }

  /** The delimiter string to use. */
  private String mDelim;

  /** Default constructor. Used only for reflection. */
  public TextFileKeyValueStore() {
    this(new Options());
  }

  /**
   * Main constructor; create a new TextFileKeyValueStore to read text files.
   *
   * @param options the options that configure the file store.
   */
  public TextFileKeyValueStore(Options options) {
    super(options);
    setDelimiter(options.getDelimiter());
  }

  /** {@inheritDoc} */
  @Override
  public void storeToConf(KeyValueStoreConfiguration conf) throws IOException {
    super.storeToConf(conf);

    if (null == mDelim || mDelim.isEmpty()) {
      throw new IOException("Cannot use empty delimiter");
    }

    conf.set(CONF_DELIMITER_KEY, mDelim);
  }

  /** {@inheritDoc} */
  @Override
  public void initFromConf(KeyValueStoreConfiguration conf) throws IOException {
    super.initFromConf(conf);

    mDelim = conf.get(CONF_DELIMITER_KEY, DEFAULT_DELIMITER);
  }

  /**
   * Sets the delimiter string to use to separate keys and values on a line of text.
   * This will throw an IllegalArgumentException if delim is null or empty.
   *
   * @param delim the delimiter string to use.
   */
  public void setDelimiter(String delim) {
    if (null == delim || delim.isEmpty()) {
      throw new IllegalArgumentException("Cannot use empty delimiter");
    }

    mDelim = delim;
  }

  /**
   * Returns the delimiter string that separates keys and values in a line.
   *
   * @return the delimiter string.
   */
  public String getDelimiter() {
    return mDelim;
  }

  /** {@inheritDoc} */
  @Override
  public KeyValueStoreReader<String, String> open() throws IOException, InterruptedException {
    return new Reader(getConf(), getExpandedInputPaths(), getDelimiter());
  }

  /**
   * Reads an entire text file of records into memory, and indexes it by the key field.
   * Key and value fields are separated by the first occurrence of the 'delimiter' string
   * in a line. If the delimiter does not exist, then the entire line is taken to be the
   * key and the value is 'null'. (Note that lines that end with the delimiter string
   * will have the non-null empty string as a value.)
   *
   * <p>Lookups for a key <i>K</i> will return the first record in the file where the key field
   * has value <i>K</i>.</p>
   */
  private static class Reader extends KeyValueStoreReader<String, String> {
    /** A map from keys to values loaded from the input files. */
    private Map<String, String> mMap;

    /** The delimiter string. */
    private final String mDelim;

    /**
     * Constructs a key value reader over a SequenceFile.
     *
     * @param conf The Hadoop configuration.
     * @param paths The path to the Avro file(s).
     * @param delim the delimeter string.
     * @throws IOException If the seqfile cannot be read.
     */
    public Reader(Configuration conf, List<Path> paths, String delim) throws IOException {
      if (null == delim || delim.isEmpty()) {
        throw new IOException("Cannot use null/empty delimiter.");
      }

      mDelim = delim;
      mMap = new TreeMap<String, String>();

      for (Path path : paths) {
        // Load the entire file into the lookup map.
        FileSystem fs = path.getFileSystem(conf);
        BufferedReader reader = null;
        try {
          reader = new BufferedReader(new InputStreamReader(fs.open(path), "UTF-8"));
          String line = reader.readLine();
          while (null != line) {
            String key;
            String val;
            int delimPos = line.indexOf(mDelim);
            if (-1 == delimPos) {
              // No delimiter in the line.
              key = line; // Whole line is the key.
              val = null; // The value is null.
            } else {
              key = line.substring(0, delimPos);
              val = line.substring(delimPos + mDelim.length());
            }

            if (!mMap.containsKey(key)) {
              mMap.put(key, val);
            }

            line = reader.readLine();
          }
        } catch (UnsupportedEncodingException uee) {
          // Can't open as UTF-8? Java is quite broken if we get here
          throw new IOException(uee);
        } finally {
          IOUtils.closeQuietly(reader);
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
    public String get(String key) throws IOException, InterruptedException {
      if (!isOpen()) {
        throw new IOException("Reader is closed");
      }
      return mMap.get(key);
    }

    /** {@inheritDoc} */
    @Override
    public boolean containsKey(String key) throws IOException, InterruptedException {
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
