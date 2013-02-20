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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.kiji.annotations.ApiAudience;

/**
 * The class TextMapReduceJobInput is used to indicate the usage of a Text file
 * in HDFS as input to a MapReduce job.
 *
 * <h2>Configuring an input:</h2>
 * <p>
 *   TextMapReduceJobInput must be configured with the paths of the files to read from.
 *   Each file specified will be read by one MapReduce split. To setup reading from a
 *   single text file:
 * </p>
 * <pre>
 *   <code>
 *     final Path inputFile = new Path("/path/to/input");
 *     final MapReduceJobInput textJobInput = new TextMapReduceJobInput(inputFile);
 *   </code>
 * </pre>
 *
 * @see KijiMapReduceJobBuilder for more information about running a MapReduce job.
 * @see KijiTableMapReduceJobOutput for more information about configuring a MapReduce
 *     job to output to a Kiji table.
 */
@ApiAudience.Public
public final class TextMapReduceJobInput extends FileMapReduceJobInput {
  /**
   * Constructs job input from a list of paths to text files.
   *
   * @param paths The paths to the job input files.
   */
  public TextMapReduceJobInput(Path... paths) {
    super(paths);
  }

  /** {@inheritDoc} */
  @Override
  protected Class<? extends InputFormat> getInputFormatClass() {
    return TextInputFormat.class;
  }
}
