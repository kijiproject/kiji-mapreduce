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

package org.kiji.mapreduce.util;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.NumberFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.aspectj.lang.Aspects;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.schema.util.LogTimerAspect;
import org.kiji.schema.util.LoggingInfo;

/**
 * This aspect is invoked after the cleanup function in a mapreduce job. It
 * accesses logging information gathered by the LogTimerAspect in kiji schema and
 * serializes it to a local file.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@Aspect
public class SerializeLoggerAspect {
  private LogTimerAspect mLogTimerAspect;
  private static final Logger LOG = LoggerFactory.getLogger(SerializeLoggerAspect.class);
  /**
   * Output directory (under map reduce job's current directory) for profiling results.
   * Example: /users/username/kijistats
   */
  private static final String STATS_DIR = "kijistats";

  /**
   * Default constructor. Initializes the pid of the JVM running the tool
   * and the singleton LogTimerAspect for this JVM instance.
   */
  protected SerializeLoggerAspect() {
    if (Aspects.hasAspect(LogTimerAspect.class)) {
      mLogTimerAspect = Aspects.aspectOf(LogTimerAspect.class);
    } else {
      throw new RuntimeException("Log Timer aspect not found!");
    }
  }

  /**
   * Pointcut attached to cleanup code in Map/Reduce tasks.
   */
  @Pointcut("execution(* org.kiji.mapreduce.KijiMapper.cleanup(..)) ||"
      + "execution(* org.kiji.mapreduce.KijiReducer.cleanup(..))")
  protected void mrCleanupPoint() {
  }

  /**
   * Logic to serialize collected profiling content to a file on HDFS. The files are stored
   * in the current working directory for this context, in a folder specified by STATS_DIR. The per
   * task file is named by the task attempt id.
   *
   * @param context The {@link TaskInputOutputContext} for this job.
   * @throws IOException If the writes to hdfs fail.
   */
  private void serializeToFile(TaskInputOutputContext context) throws IOException {
    Path parentPath = new Path(context.getWorkingDirectory(), STATS_DIR);
    FileSystem fs = FileSystem.get(context.getConfiguration());
    fs.mkdirs(parentPath);
    Path path = new Path(parentPath, context.getTaskAttemptID().toString());
    OutputStreamWriter out = new OutputStreamWriter(fs.create(path, true), "UTF-8");
    try {
      out.write("Job Name, Job ID, Task Attempt, Function Signature, Aggregate Time (nanoseconds), "
          + "Number of Invocations, Time per call (nanoseconds)\n");
      ConcurrentHashMap<String, LoggingInfo> signatureTimeMap =
          mLogTimerAspect.getSignatureTimeMap();
      for (Map.Entry<String, LoggingInfo> entrySet: signatureTimeMap.entrySet()) {
        // ensure that files do not end up with x.yzE7 format for floats
        NumberFormat nf = NumberFormat.getInstance();
        nf.setGroupingUsed(false);
        nf.setMinimumFractionDigits(1);
        nf.setMaximumFractionDigits(3);

        out.write(context.getJobName() + ", "
            + context.getJobID() + ", "
            + context.getTaskAttemptID() + ", "
            + entrySet.getKey() + ", "
            + entrySet.getValue().toString() + ", "
            + nf.format(entrySet.getValue().perCallTime()) + "\n");
      }
    } finally {
      out.close();
    }
  }

  /**
   * Advice for running after any functions that match PointCut "writeResultsLocal".
   *
   * @param thisJoinPoint The joinpoint that matched the pointcut.
   */
  @AfterReturning("mrCleanupPoint() && !cflowbelow(mrCleanupPoint())")
  public void afterMRCleanup(final JoinPoint thisJoinPoint) {
    TaskInputOutputContext context = (TaskInputOutputContext)thisJoinPoint.getArgs()[0];
    try {
      serializeToFile(context);
    } catch (IOException ioe) {
      LOG.error("Failure writing profiling results", ioe);
    }
  }
}
