/**
 * (c) Copyright 2013 WibiData, Inc.
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

package org.kiji.mapreduce.testlib;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.Map;
import java.util.Random;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.HFileLoader;
import org.kiji.mapreduce.KijiMRTestLayouts;
import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.gather.KijiGatherJobBuilder;
import org.kiji.mapreduce.output.MapReduceJobOutputs;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;

/** Tests bulk-importers. */
public class IntegrationTestTableMapper extends AbstractKijiIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestTableMapper.class);

  private Configuration mConf = null;
  private FileSystem mFS = null;
  private Kiji mKiji = null;
  private KijiTable mInputTable = null;
  private KijiTable mOutputTable = null;

  /**
   * Generates a random HDFS path.
   *
   * @param prefix Prefix for the random file name.
   * @return a random HDFS path.
   * @throws Exception on error.
   */
  private Path makeRandomPath(String prefix) throws Exception {
    Preconditions.checkNotNull(mFS);
    final Path base = new Path(FileSystem.getDefaultUri(mConf));
    final Random random = new Random(System.nanoTime());
    return new Path(base, String.format("/%s-%s", prefix, random.nextLong()));
  }

  /**
   * Reads a table into a map from Kiji row keys to KijiRowData.
   *
   * @param table Kiji table to read from.
   * @param kdr Kiji data request.
   * @return a map of the rows.
   * @throws Exception on error.
   */
  private static Map<String, KijiRowData> toRowMap(KijiTable table, KijiDataRequest kdr)
      throws Exception {
    final KijiTableReader reader = table.openTableReader();
    try {
      final KijiRowScanner scanner = reader.getScanner(kdr);
      try {
        final Map<String, KijiRowData> rows = Maps.newHashMap();
        for (KijiRowData row : scanner) {
          rows.put(Bytes.toString((byte[]) row.getEntityId().getComponentByIndex(0)), row);
        }
        return rows;
      } finally {
        scanner.close();
      }
    } finally {
      reader.close();
    }
  }

  private void populateInputTable() throws Exception {
    final KijiTable table = mInputTable;
    final KijiTableWriter writer = table.openTableWriter();
    writer.put(table.getEntityId("1"), "info", "first_name", "Marsellus");
    writer.put(table.getEntityId("1"), "info", "last_name", "Wallace");
    writer.put(table.getEntityId("1"), "info", "zip_code", 94110);

    writer.put(table.getEntityId("2"), "info", "first_name", "Vincent");
    writer.put(table.getEntityId("2"), "info", "last_name", "Vega");
    writer.put(table.getEntityId("2"), "info", "zip_code", 94110);

    writer.put(table.getEntityId("3"), "info", "first_name", "Jules");
    writer.put(table.getEntityId("3"), "info", "last_name", "Winnfield");
    writer.put(table.getEntityId("3"), "info", "zip_code", 93221);
    writer.close();
  }

  @Before
  public final void setupIntegrationTestTableMapper() throws Exception {
    mConf = getConf();
    mFS = FileSystem.get(mConf);

    mKiji = Kiji.Factory.open(getKijiURI(), mConf);
    final String inputTableName = "input";
    final String outputTableName = "output";

    mKiji.createTable(KijiMRTestLayouts.getTestLayout(inputTableName));
    mKiji.createTable(KijiMRTestLayouts.getTestLayout(outputTableName));

    mInputTable = mKiji.openTable(inputTableName);
    mOutputTable = mKiji.openTable(outputTableName);

    populateInputTable();
  }

  @After
  public final void teardownIntegrationTestTableMapper() throws Exception {
    mInputTable.release();
    mOutputTable.release();
    mKiji.release();
    // NOTE: fs should get closed here, but doesn't because of a bug with FileSystem that
    // causes it to close other thread's filesystem objects. For more information
    // see: https://issues.apache.org/jira/browse/HADOOP-7973

    mInputTable = null;
    mOutputTable = null;
    mKiji = null;
    mFS = null;
    mConf = null;
  }

  @Test
  public void testSimpleTableMapperDirect() throws Exception {
    final KijiMapReduceJob mrjob = KijiGatherJobBuilder.create()
        .withConf(mConf)
        .withGatherer(SimpleTableMapperAsGatherer.class)
        .withInputTable(mInputTable.getURI())
        .withOutput(MapReduceJobOutputs.newDirectKijiTableMapReduceJobOutput(mOutputTable.getURI()))
        .build();
    assertTrue(mrjob.run());

    validateOutputTable();
  }

  @Test
  public void testSimpleTableMapperHFiles() throws Exception {
    final Path hfileDirPath = this.makeRandomPath("hfile-output");
    try {
      final KijiMapReduceJob mrjob = KijiGatherJobBuilder.create()
          .withConf(mConf)
          .withGatherer(SimpleTableMapperAsGatherer.class)
          .withInputTable(mInputTable.getURI())
          .withOutput(MapReduceJobOutputs.newHFileMapReduceJobOutput(
              mOutputTable.getURI(), hfileDirPath, 1))
          .build();
      assertTrue(mrjob.run());

      final HFileLoader loader = HFileLoader.create(mConf);
      // There is only one reducer, hence one HFile shard:
      final Path hfilePath = new Path(hfileDirPath, "part-r-00000.hfile");
      loader.load(hfilePath, mOutputTable);

      validateOutputTable();

    } finally {
      mFS.delete(hfileDirPath, true);
    }
  }

  private void validateOutputTable() throws Exception {
    final KijiDataRequestBuilder okdrb = KijiDataRequest.builder();
    okdrb.newColumnsDef().withMaxVersions(3).addFamily("primitives");
    final KijiDataRequest okdr = okdrb.build();
    final Map<String, KijiRowData> rows = toRowMap(mOutputTable, okdr);
    assertEquals(2, rows.size());
    final Collection<CharSequence> peopleIn94110 =
        rows.get("94110").<CharSequence>getValues("primitives", "string").values();
    assertEquals(2, peopleIn94110.size());
    final Collection<CharSequence> peopleIn93221 =
        rows.get("93221").<CharSequence>getValues("primitives", "string").values();
    assertEquals(1, peopleIn93221.size());
    assertEquals("Jules Winnfield", peopleIn93221.iterator().next().toString());
  }
}
