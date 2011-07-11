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

package com.memonews.hbase.util;

import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility-Functions for HBaseAdmin.
 * 
 * @author nkuebler, MeMo News AG
 */
public class HBaseAdminUtil {

	private static final Logger LOG = LoggerFactory
			.getLogger(HBaseAdminUtil.class);

	public static void copyTable(final Configuration conf,
			final String sourceTableName, final String targetTableName)
			throws Exception {
		final HBaseAdmin admin = new HBaseAdmin(new Configuration(conf));
		final HTable sourceTable = new HTable(conf, sourceTableName);
		final HTableDescriptor target = cloneTableDescriptor(
				sourceTable.getTableDescriptor(), targetTableName);

		LOG.info("creating table ... " + Bytes.toString(target.getName()));
		admin.createTable(target);

		final HColumnDescriptor[] families = sourceTable.getTableDescriptor()
				.getColumnFamilies();
		for (final HColumnDescriptor family : families) {
			copyColumnFamily(conf, sourceTableName, family.getNameAsString(),
					targetTableName, family.getNameAsString());
		}
	}

	public static void moveColumnFamily(final Configuration conf,
			final String sourceTableName, final String sourceColumnFamily,
			final String targetTableName, final String targetColumnFamily)
			throws Exception {
		final HBaseAdmin admin = new HBaseAdmin(new Configuration(conf));
		final HTable sourceTable = new HTable(conf, sourceTableName);
		final HColumnDescriptor source = findColumnFamily(sourceTable,
				sourceColumnFamily);

		copyColumnFamily(conf, sourceTableName, sourceColumnFamily,
				targetTableName, targetColumnFamily);

		LOG.info("deleting column-family ... "
				+ Bytes.toString(source.getName()));
		admin.disableTable(sourceTableName);
		admin.deleteColumn(Bytes.toBytes(sourceTableName), source.getName());
		admin.enableTable(sourceTableName);
	}

	public static void copyColumnFamily(final Configuration conf,
			final String sourceTableName, final String sourceColumnFamily,
			final String targetTableName, final String targetColumnFamily)
			throws Exception {
		final HBaseAdmin admin = new HBaseAdmin(new Configuration(conf));

		final HTable sourceTable = new HTable(conf, sourceTableName);
		HTable targetTable = new HTable(conf, targetTableName);
		final HColumnDescriptor source = findColumnFamily(sourceTable,
				sourceColumnFamily);
		if (source == null) {
			throw new Exception("Source Column Family '" + sourceColumnFamily
					+ "' not found");
		}
		if (null != findColumnFamily(targetTable, targetColumnFamily)) {
			throw new Exception("Target Column Family '" + targetColumnFamily
					+ "' already exists");
		}

		final HColumnDescriptor target = cloneColumnDescriptor(source,
				targetColumnFamily);
		LOG.info("creating column-family ... "
				+ Bytes.toString(target.getName()));
		admin.disableTable(targetTableName);
		admin.addColumn(targetTableName, target);
		admin.enableTable(targetTableName);

		targetTable = new HTable(conf, targetTableName);
		
		HBaseUtil.copyColumnFamilyData(conf, sourceTableName, sourceColumnFamily, targetTableName, targetColumnFamily);
	}
	

	public static HTableDescriptor cloneTableDescriptor(
			final HTableDescriptor source, final String targetName) {
		final HTableDescriptor dest = new HTableDescriptor(targetName);
		dest.setDeferredLogFlush(source.isDeferredLogFlush());
		dest.setMaxFileSize(source.getMaxFileSize());
		dest.setMemStoreFlushSize(source.getMemStoreFlushSize());
		dest.setReadOnly(dest.isReadOnly());
		return dest;
	}

	public static HColumnDescriptor cloneColumnDescriptor(
			final HColumnDescriptor source, final String targetName) {
		final HColumnDescriptor dest = new HColumnDescriptor(targetName);
		dest.setBlockCacheEnabled(source.isBlockCacheEnabled());
		dest.setBlocksize(source.getBlocksize());
		dest.setBloomFilterType(source.getBloomFilterType());
		dest.setCompactionCompressionType(source.getCompactionCompressionType());
		dest.setCompressionType(source.getCompressionType());
		dest.setInMemory(source.isInMemory());
		dest.setMaxVersions(source.getMaxVersions());
		dest.setScope(source.getScope());
		dest.setTimeToLive(source.getTimeToLive());
		return dest;
	}

	public static HColumnDescriptor findColumnFamily(final HTable table,
			final String columnFamily) throws Exception {
		for (final HColumnDescriptor desc : table.getTableDescriptor()
				.getFamilies()) {
			if (Bytes.toString(desc.getName()).equals(columnFamily)) {
				return desc;
			}
		}
		return null;
	}

}
