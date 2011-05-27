/**
 * HBase Commandline Utilities
 * 
 * Copyright (C) 2011 MeMo News AG
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.memonews.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.GenericOptionsParser;

import com.memonews.hbase.util.HBaseAdminUtil;

/**
 * HBase TAble Copy Utility.
 * 
 * @author nkuebler, MeMo News AG
 */
public class CopyTable {

    /**
     * Creates an 1:1 duplicate of a table with all it's data.
     * 
     * @param args
     *            cli-parameter
     * @throws Exception
     *             when an error occurs
     */
    public static void main(final String[] args) throws Exception {
	final Configuration conf = HBaseConfiguration.create();
	new GenericOptionsParser(conf, args);
	if (args.length != 2) {
	    System.out.println(getUsage());
	} else {
	    HBaseAdminUtil.copyTable(conf, args[0], args[1]);
	}
    }

    private static String getUsage() {
	return "CopyTable <source-table> <target-table>";
    }
}
