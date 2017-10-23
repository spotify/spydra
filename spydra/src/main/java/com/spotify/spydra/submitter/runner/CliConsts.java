/*
 * Copyright 2017 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.spydra.submitter.runner;

/**
 * Constants frequently used for CLI sub-commands and options
 */
public class CliConsts {
  public static final String SUBMIT_CMD_NAME = "submit";
  public static final String LOGS_CMD_NAME = "dump-logs";
  public static final String DUMP_HISTORY_CMD_NAME = "dump-history";
  public static final String RUN_JHS_CMD_NAME = "run-jhs";
  public final static String JOB_ID_OPTION_NAME = "application";
  public final static String CLIENT_ID_OPTION_NAME = "clientid";
  public final static String JAR_OPTION_NAME = "jar";
  public final static String JARS_OPTION_NAME = "jars";
  public final static String SPYDRA_JSON_OPTION_NAME = "spydra-json";
  public final static String USERNAME_OPTION_NAME = "username";
  public final static String LOG_BUCKET_OPTION_NAME = "log-bucket";
  public final static String JOBNAME_OPTION_NAME = "job-name";
  public final static String LABELS_OPTION_NAME = "labels";
}
