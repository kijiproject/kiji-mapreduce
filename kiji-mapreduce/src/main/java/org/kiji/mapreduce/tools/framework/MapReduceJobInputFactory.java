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

package org.kiji.mapreduce.tools.framework;

import java.io.IOException;
import java.util.Map;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.MapReduceJobInput;
import org.kiji.mapreduce.input.AvroKeyMapReduceJobInput;
import org.kiji.mapreduce.input.AvroKeyValueMapReduceJobInput;
import org.kiji.mapreduce.input.HTableMapReduceJobInput;
import org.kiji.mapreduce.input.KijiTableMapReduceJobInput;
import org.kiji.mapreduce.input.SequenceFileMapReduceJobInput;
import org.kiji.mapreduce.input.TextMapReduceJobInput;
import org.kiji.mapreduce.input.WholeTextFileMapReduceJobInput;
import org.kiji.mapreduce.tools.framework.JobInputSpec.Format;
import org.kiji.schema.tools.SpaceSeparatedMapParser;

/**
 * Constructs instances of MapReduceJobInput from a string specification.
 *
 * See {@link JobInputSpec} for the full inputspec specification.
 */
@ApiAudience.Framework
public final class MapReduceJobInputFactory {
  /**
   * Constructs a new factory for MapReduce job inputs.
   */
  private MapReduceJobInputFactory() { }

  /**
   * Creates a new factory for instances of {@link MapReduceJobInput}.
   *
   * @return a new factory for MapReduce job inputs.
   */
  public static MapReduceJobInputFactory create() {
    return new MapReduceJobInputFactory();
  }

  /**
   * Creates a job input from a command-line flag space-separated parameters map.
   *
   * @param ssm Space-separated parameters map from the command-line.
   * @return a job input configured from the command-line parameters.
   * @throws IOException on I/O error.
   */
  public MapReduceJobInput fromSpaceSeparatedMap(String ssm) throws IOException {
    final Map<String, String> params = SpaceSeparatedMapParser.create().parse(ssm);
    try {
      final MapReduceJobInput input =
          createJobInput(Format.parse(params.get(JobIOConfKeys.FORMAT_KEY)));
      input.initialize(params);
      return input;
    } catch (JobIOSpecParseException exn) {
      throw new IOException(String.format(
          "Invalid job output specification: '%s': %s", ssm, exn.getMessage()), exn);
    }
  }

  /**
   * Instantiates the appropriate job output.
   *
   * @param format Format of the job input.
   * @return an unconfigured job input instance.
   * @throws IOException on I/O error.
   */
  private MapReduceJobInput createJobInput(Format format) throws IOException {
    switch (format) {
    case HTABLE:
      return new HTableMapReduceJobInput();
    case TEXT:
      return new TextMapReduceJobInput();
    case SMALL_TEXT_FILES:
      return new WholeTextFileMapReduceJobInput();
    case SEQUENCE:
      return new SequenceFileMapReduceJobInput();
      // TODO(KIJIMR-61) Map file job input
    // case MAP_FILE:
    //   throw new IOException(String.format(
    //       "Map files are not supported as job input in spec '%s'.", ssm));
    case AVRO:
      return new AvroKeyMapReduceJobInput();
    case AVRO_KV:
      return new AvroKeyValueMapReduceJobInput();
    case KIJI:
      return new KijiTableMapReduceJobInput();
    default:
      throw new RuntimeException(String.format("Unhandled job output format: '%s'.", format));
    }
  }

}
