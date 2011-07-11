/*
 * Copyright 2011 MeMo News AG. All rights reserved.
 */
package com.memonews.hbase.util;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility-Functions for HBase.
 * 
 * @author nkuebler, MeMo News AG
 */
public class HBaseUtil {

	private static final Logger LOG = LoggerFactory.getLogger(HBaseUtil.class);

	public static void moveRow(final Configuration conf,
			final String sourceTableName, final String sourceKey,
			final String targetTableName, final String targetKey)
			throws Exception {
		copyRow(conf, sourceTableName, sourceKey, targetTableName, targetKey);
		deleteRow(conf, sourceTableName, sourceKey);
	}

	public static void deleteRow(final Configuration conf,
			final String tableName, final String key) throws IOException {
		final HTable table = new HTable(conf, tableName);
		Delete delete = new Delete(Bytes.toBytes(key));
		LOG.info("deleting row " + key + " in table " + tableName);
		table.delete(delete);
	}

	public static void copyRow(final Configuration conf,
			final String sourceTableName, final String sourceKey,
			final String targetTableName, final String targetKey)
			throws Exception {
		final HTable sourceTable = new HTable(conf, sourceTableName);
		Get get = new Get(Bytes.toBytes(sourceKey));
		get.setMaxVersions();
		Result result = sourceTable.get(get);
		Set<byte[]> allFamilies = result.getMap().keySet();
		Put put = new Put(Bytes.toBytes(targetKey));
		int familyCount = 0;
		for (byte[] family : allFamilies) {
			resultToPut(result, put, family, family);
			familyCount++;
		}
		LOG.info("creating row with " + familyCount + " families in table "
				+ targetTableName);
		final HTable targetTable = new HTable(conf, targetTableName);
		targetTable.put(put);
	}

	public static void copyColumnFamilyData(final Configuration conf,
			final String sourceTableName, final String sourceColumnFamily,
			final String targetTableName, final String targetColumnFamily)
			throws IOException {
		LOG.info("copy column data ... from " + sourceTableName + "/"
				+ sourceColumnFamily + " to " + targetTableName + "/"
				+ targetColumnFamily);
		final HTable sourceTable = new HTable(conf, sourceTableName);
		final HTable targetTable = new HTable(conf, targetTableName);
		byte[] sourceName = Bytes.toBytes(sourceColumnFamily);
		byte[] targetName = Bytes.toBytes(sourceColumnFamily);

		final Scan scan = new Scan();
		scan.addFamily(sourceName);
		scan.setMaxVersions();
		scan.setBatch(1000);

		final ResultScanner scanner = sourceTable.getScanner(scan);
		int i = 0;
		for (final Result result : scanner) {
			final Put put = new Put(result.getRow());
			HBaseUtil.resultToPut(result, put, sourceName, targetName);
			System.out.print(".");
			if (i++ > 1000) {
				System.out.println(" at row:" + Bytes.toString(put.getRow()));
				i = 0;
			}
			try {
				targetTable.put(put);
			} catch (IOException e) {
				LOG.error("couldn't copy row: " + e.getMessage());
			}
		}
	}

	public static Put resultToPut(final Result source, Put target,
			final byte[] sourceFamily, final byte[] targetFamily) {
		final NavigableMap<byte[], NavigableMap<Long, byte[]>> map = source
				.getMap().get(sourceFamily);
		for (final byte[] qualifier : map.keySet()) {
			for (final Long ts : map.get(qualifier).keySet()) {
				final byte[] value = map.get(qualifier).get(ts);
				target.add(targetFamily, qualifier, ts, value);
			}
		}
		return target;
	}

}
