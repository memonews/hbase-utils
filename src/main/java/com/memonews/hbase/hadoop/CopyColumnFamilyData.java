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

package com.memonews.hbase.hadoop;

import com.memonews.hbase.util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * HBase Row Copy Utility.
 * 
 * @author nkuebler, MeMo News AG
 */
public class CopyColumnFamilyData {

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

		final String[] remainingArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (remainingArgs.length != 4) {
			System.out.println(getUsage());
            System.exit(1);
            return;
		}

        String sourceTableName = remainingArgs[0];
        String sourceColumnFamily = remainingArgs[1];
        String destinationTableName = remainingArgs[2];
        String destinationColumnFamily = remainingArgs[3];

        Job job = new Job(conf);

        job.getConfiguration().set("sourceColumnFamily", sourceColumnFamily);
        job.getConfiguration().set("destinationColumnFamily", destinationColumnFamily);

        job.setJarByClass(CopyColumnFamilyData.class);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(sourceColumnFamily));
        TableMapReduceUtil.setScannerCaching(job, 10000);
        TableMapReduceUtil.initTableMapperJob(sourceTableName, scan, IdentityTableMapper.class, ImmutableBytesWritable.class, Result.class, job);

        TableMapReduceUtil.initTableReducerJob(
                destinationTableName,
                ResultToPutIdentityReducer.class,
                job);

        // determine the number of reduce tasks based on the number of splits in the source table
        // rather than the destination table since the destination table will generally be empty
        TableMapReduceUtil.setNumReduceTasks(sourceTableName, job);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

    public static class ResultToPutIdentityReducer extends TableReducer<ImmutableBytesWritable, Result, NullWritable> {
        byte[] destinationColumnFamily;
        byte[] sourceColumnFamily;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            destinationColumnFamily = Bytes.toBytes(context.getConfiguration().get("destinationColumnFamily"));
            sourceColumnFamily = Bytes.toBytes(context.getConfiguration().get("sourceColumnFamily"));
        }

        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<Result> values, Context context) throws IOException, InterruptedException {
            for (Result result : values) {
                Put put = new Put(result.getRow());
                context.write(NullWritable.get(), HBaseUtil.resultToPut(result, put, sourceColumnFamily, destinationColumnFamily));
            }
        }
    }

	private static String getUsage() {
		return "hadoop jar target/hbase-utils-1.0-SNAPSHOT.jar com.memonews.hbase.CopyColumnFamilyData [-conf ...] <source-table> <source-row> <target-table> <target-row>";
	}


}
