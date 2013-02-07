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

import com.google.common.base.Preconditions;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.Inheritance;
import org.kiji.common.flags.Flag;
import org.kiji.mapreduce.KijiTableInputJobBuilder;
import org.kiji.mapreduce.input.KijiTableMapReduceJobInput;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.tools.ToolUtils;

/**
 * Base class for tools that run MapReduce jobs over kiji tables.
 *
 * @param <B> The type of job builder to use.
 */
@ApiAudience.Framework
@Inheritance.Extensible
public abstract class KijiJobTool<B extends KijiTableInputJobBuilder> extends JobTool<B> {
  // TODO: Update usage doc for entity IDs:
  @Flag(name="start-row", usage="The row to start scanning at (inclusive)")
  protected String mStartRow = "";

  @Flag(name="limit-row", usage="The row to stop scanning at (exclusive)")
  protected String mLimitRow = "";

  /** Job input must be a Kiji table. */
  private KijiTableMapReduceJobInput mJobInput;

  /** Creates a new <code>KijiTool</code> instance. */
  protected KijiJobTool() {
  }

  /** {@inheritDoc} */
  @Override
  protected void validateFlags() throws Exception {
    // Parse --input and --output flags:
    super.validateFlags();

    Preconditions.checkArgument(getJobInput() instanceof KijiTableMapReduceJobInput,
        "Invalid job input '%s' : input must be a Kiji table.", mInputFlag);

    mJobInput = (KijiTableMapReduceJobInput) getJobInput();
  }

  /** {@inheritDoc} */
  @Override
  protected void configure(B jobBuilder)
      throws ClassNotFoundException, IOException, JobIOSpecParseException {
    // Basic job configuration (base JobConf, jars and KV stores):
    super.configure(jobBuilder);

    // Configure job input:
    jobBuilder.withJobInput(mJobInput);

    final Kiji kiji = Kiji.Factory.open(mJobInput.getInputTableURI());
    try {
      final KijiTable table = kiji.openTable(mJobInput.getInputTableURI().getTable());
      try {
        final EntityIdFactory eidFactory = table.getEntityIdFactory();
        if (!mStartRow.isEmpty()) {
          jobBuilder.withStartRow(eidFactory.fromHBaseRowKey(ToolUtils.parseRowKeyFlag(mStartRow)));
        }
        if (!mLimitRow.isEmpty()) {
          jobBuilder.withLimitRow(eidFactory.fromHBaseRowKey(ToolUtils.parseRowKeyFlag(mLimitRow)));
        }
      } finally {
        table.close();
      }
    } finally {
      kiji.release();
    }
  }

  /** @return the input for this job, which must be a Kiji table. */
  protected KijiTableMapReduceJobInput getJobInputTable() {
    return mJobInput;
  }
}
