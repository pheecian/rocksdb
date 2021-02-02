// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

import java.util.ArrayList;
import java.util.List;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

public class RocksDBColumnFamilySample {
  static {
    RocksDB.loadLibrary();
  }

  public static void main(final String[] args) throws RocksDBException {
    if (args.length < 1) {
      System.out.println(
          "usage: RocksDBColumnFamilySample db_path");
      System.exit(-1);
    }

    final String db_path = args[0];

    System.out.println("RocksDBColumnFamilySample");
    try(final Options options = new Options().setCreateIfMissing(true);
        final RocksDB db = RocksDB.open(options, db_path)) {

      assert(db != null);

      // create column family
      try(final ColumnFamilyHandle columnFamilyHandle = db.createColumnFamily(
          new ColumnFamilyDescriptor("new_cf".getBytes(),
          new ColumnFamilyOptions()))) {
        assert (columnFamilyHandle != null);
      }
    }

    // open DB with two column families with different write buffer size
    final List<ColumnFamilyDescriptor> columnFamilyDescriptors =
        new ArrayList<>();
    // have to open default column family
    columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,
        new ColumnFamilyOptions().setWriteBufferSize(8 * SizeUnit.MB)));
    // open the new one, too
    columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
        "new_cf".getBytes(), new ColumnFamilyOptions().setWriteBufferSize(16 * SizeUnit.MB)));
    final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
    try(final DBOptions options = new DBOptions();
        final RocksDB db = RocksDB.open(options, db_path,
            columnFamilyDescriptors, columnFamilyHandles)) {
      assert(db != null);

      try {
        // put and get from non-default column family
        db.put(
            columnFamilyHandles.get(1), new WriteOptions(), "key".getBytes(), "value".getBytes());

        // atomic write
        try (final WriteBatch wb = new WriteBatch()) {
          wb.put(columnFamilyHandles.get(0), "key2".getBytes(),
              "value2".getBytes());
          wb.put(columnFamilyHandles.get(1), "key3".getBytes(),
              "value3".getBytes());
          wb.delete(columnFamilyHandles.get(1), "key".getBytes());
          db.write(new WriteOptions(), wb);
        }

      } finally {
        for (final ColumnFamilyHandle handle : columnFamilyHandles) {
          handle.close();
        }
      }
    }
    columnFamilyDescriptors.clear();
    final Cache cache = new LRUCache(1 * SizeUnit.MB);
    final TableFormatConfig conf = new BlockBasedTableConfig().setBlockCache(cache);
    // have to open default column family
    columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
        RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions().setTableFormatConfig(conf)));
    // open the new one, too
    columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
        "new_cf".getBytes(), new ColumnFamilyOptions().setTableFormatConfig(conf)));
    columnFamilyHandles.clear();
    try (
        // cost write buffer size from all column families to the block cache shared by all column
        // families
        final WriteBufferManager manager = new WriteBufferManager(0l, cache);
        final DBOptions options = new DBOptions().setWriteBufferManager(manager);
        final RocksDB db =
            RocksDB.open(options, db_path, columnFamilyDescriptors, columnFamilyHandles)) {
      assert (db != null);
      try {
        db.put(
            columnFamilyHandles.get(1), new WriteOptions(), "key".getBytes(), "value".getBytes());
        db.put(
                columnFamilyHandles.get(1), new WriteOptions(), "key1".getBytes(), "value".getBytes());
        db.put(
                columnFamilyHandles.get(1), new WriteOptions(), "key2".getBytes(), "value".getBytes());
        db.put(
                columnFamilyHandles.get(1), new WriteOptions(), "key3".getBytes(), "value".getBytes());
        db.put(
                columnFamilyHandles.get(1), new WriteOptions(), "key4".getBytes(), "value".getBytes());
        System.out.print(cache.getUsage());
        // drop column family
        db.dropColumnFamily(columnFamilyHandles.get(1));
      } finally {
        for (final ColumnFamilyHandle handle : columnFamilyHandles) {
          handle.close();
        }
      }
    }
  }
}
