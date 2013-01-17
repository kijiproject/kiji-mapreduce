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

package org.kiji.mapreduce.input;

import java.io.IOException;

import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.HTableInputFormat;
import org.kiji.mapreduce.MapReduceJobInput;

/** Job input that reads from an HTable (an HBase table). */
@ApiAudience.Public
public class HTableMapReduceJobInput extends MapReduceJobInput {
  /** The name of the HTable to use as job input. */
  private final String mTableName;

  /**
   * Creates a new <code>HTableMapReduceJobInput</code> instance.
   *
   * @param tableName The name of the HBase table (HTable) to use as input for the job.
   */
  public HTableMapReduceJobInput(String tableName) {
    mTableName = tableName;
  }

  /** {@inheritDoc} */
  @Override
  public void configure(Job job) throws IOException {
    // Configure the input format class.
    super.configure(job);

    // Configure the input HTable name.
    job.getConfiguration().set(TableInputFormat.INPUT_TABLE, mTableName);
  }

  /** {@inheritDoc} */
  @Override
  protected Class<? extends InputFormat<?, ?>> getInputFormatClass() {
    return HTableInputFormat.class;
  }
}
