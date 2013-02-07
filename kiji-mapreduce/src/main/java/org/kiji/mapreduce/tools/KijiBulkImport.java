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

package org.kiji.mapreduce.tools;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Preconditions;
import com.sun.tools.corba.se.idl.InvalidArgument;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.common.flags.Flag;
import org.kiji.mapreduce.KijiBulkImportJobBuilder;
import org.kiji.mapreduce.KijiBulkImporter;
import org.kiji.mapreduce.MapReduceJobInput;
import org.kiji.mapreduce.MapReduceJobOutput;
import org.kiji.mapreduce.output.DirectKijiTableMapReduceJobOutput;
import org.kiji.mapreduce.output.HFileMapReduceJobOutput;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.tools.KijiToolLauncher;
import org.kiji.schema.tools.RequiredFlagException;

/** Bulk imports a file into a Kiji table. */
@ApiAudience.Private
public final class KijiBulkImport extends JobTool<KijiBulkImportJobBuilder> {
  private static final Logger LOG = LoggerFactory.getLogger(KijiBulkImport.class);

  @Flag(name="importer", usage="KijiBulkImporter class to use")
  private String mImporter = "";

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return "bulk-import";
  }

  /** {@inheritDoc} */
  @Override
  public String getDescription() {
    return "Bulk import data into a table";
  }

  /** {@inheritDoc} */
  @Override
  public String getCategory() {
    return "Bulk";
  }

  /** URI of the table data will be written to. */
  private KijiURI mOutputTableURI;

  /** Kiji instance where the output table lives. */
  private Kiji mKiji;

  /** KijiTable to import data into. */
  private KijiTable mTable;

  /** {@inheritDoc} */
  @Override
  protected void validateFlags() throws Exception {
    // Do NOT call super.validateFlags()

    if (mInputFlag.isEmpty()) {
      throw new RequiredFlagException("input");
    }
    if (mImporter.isEmpty()) {
      throw new RequiredFlagException("importer");
    }
    if (mOutputFlag.isEmpty()) {
      throw new RequiredFlagException("output");
    }

    mInputSpec = JobInputSpec.parse(mInputFlag);
    mOutputSpec = JobOutputSpec.parse(mOutputFlag);
    switch (mOutputSpec.getFormat()) {
    case HFILE: {
      final String[] locSplit = mOutputSpec.getLocation().split(",", 2);
      Preconditions.checkArgument(locSplit.length == 2);
      mOutputTableURI = KijiURI.newBuilder(locSplit[1]).build();
      break;
    }
    case KIJI: {
      mOutputTableURI = KijiURI.newBuilder(mOutputSpec.getLocation()).build();
      break;
    }
    default: throw new InvalidArgument(String.format(
        "Bulk-import jobs output must be one of {'hfile', 'kiji'}, but got %s",
        mOutputFlag));
    }
    Preconditions.checkArgument(mOutputTableURI.getTable() != null,
        "Specify the table to import data into with --output=...");
  }

  /** {@inheritDoc} */
  @Override
  protected void setup() throws Exception {
    super.setup();
    mKiji = Kiji.Factory.open(mOutputTableURI);
    mTable = mKiji.openTable(mOutputTableURI.getTable());
  }

  /** {@inheritDoc} */
  @Override
  protected void cleanup() throws IOException {
    mTable.close();
    mKiji.release();
    super.cleanup();
  }

  /** {@inheritDoc} */
  @Override
  protected KijiBulkImportJobBuilder createJobBuilder() {
    return KijiBulkImportJobBuilder.create();
  }

  /** {@inheritDoc} */
  @Override
  protected void configure(KijiBulkImportJobBuilder jobBuilder)
      throws ClassNotFoundException, IOException, JobIOSpecParseException {

    // Resolve job input:
    final MapReduceJobInput input =
        MapReduceJobInputFactory.create().createFromInputSpec(mInputSpec);

    // Resolve job output:
    MapReduceJobOutput output = null;
    switch (mOutputSpec.getFormat()) {
    case KIJI: output = new DirectKijiTableMapReduceJobOutput(mTable);
      break;
    case HFILE: output = new HFileMapReduceJobOutput(
              mTable, new Path(mOutputSpec.getLocation()), mOutputSpec.getSplits());
      break;
    default:
      throw new RuntimeException("Invalid job output: " + mOutputSpec);
    }

    // Configure job:
    super.configure(jobBuilder);
    jobBuilder
        .withBulkImporter(KijiBulkImporter.forName(mImporter))
        .withInput(input)
        .withOutput(output);
  }

  /** {@inheritDoc} */
  @Override
  protected int run(List<String> nonFlagArgs) throws Exception {
    int jobStatus = super.run(nonFlagArgs);

    if (JobOutputSpec.Format.KIJI == mOutputSpec.getFormat()) {
      // Imported directly to the table, no need to do anything else.
      LOG.info("Bulk import complete.");
      return jobStatus;
    }

    if (0 != jobStatus) {
      LOG.error("HFiles were not generated successfully.");
    } else {
      // Provide instructions for completing the bulk import.
      if (JobOutputSpec.Format.HFILE == mOutputSpec.getFormat()) {
        // TODO(KIJIMR-58): fix job input/output specs
        LOG.info("To complete loading of job results into table, run the kiji bulk-load command");
        LOG.info("    e.g. kiji bulk-load --table=" + mOutputFlag + " --input="
            + mOutputSpec.getLocation() + (getURI().getInstance() == null ? "" : " --instance="
                + getURI().getInstance()));
      }
    }

    return jobStatus;
  }

  /**
   * Program entry point.
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(new KijiToolLauncher().run(new KijiBulkImport(), args));
  }
}
