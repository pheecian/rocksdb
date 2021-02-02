// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

import java.io.File;
import org.rocksdb.*;

public class BackupDBSample {
  static {
    RocksDB.loadLibrary();
  }

  private static final String dbPath = "/tmp/rocksdb_backup_example";
  private static final String dbPathBackup = "/tmp/rocksdb_backup_example_backup";
  private static final String dbPathRestore = "/tmp/rocksdb_backup_example_restore";
  public static void main(final String[] args) {
    File dbPathBackupFile = new File(dbPathBackup);
    if (!dbPathBackupFile.exists()) {
      dbPathBackupFile.mkdir();
    }
    File dbPathRestoreFile = new File(dbPathRestore);
    if (!dbPathRestoreFile.exists()) {
      dbPathRestoreFile.mkdir();
    }
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options, dbPath)) {
      db.put("hello".getBytes(), "world".getBytes());
      db.put("test".getBytes(), "backup".getBytes());
      try (final BackupableDBOptions bopt = new BackupableDBOptions(dbPathBackup);
           final BackupEngine be = BackupEngine.open(options.getEnv(), bopt)) {
        be.createNewBackup(db, true);
        db.put("key1".getBytes(), "value1".getBytes());
        db.put("key2".getBytes(), "value2".getBytes());
        be.createNewBackup(db, true);
      }
    } catch (final RocksDBException e) {
      System.out.format("[ERROR] caught the unexpected exception -- %s\n", e);
      assert (false);
    }
    try (final Options options = new Options().setCreateIfMissing(true);
         final BackupableDBOptions bopt = new BackupableDBOptions(dbPathBackup);
         final BackupEngine be = BackupEngine.open(options.getEnv(), bopt);
         final RestoreOptions ropts = new RestoreOptions(false);) {
      be.restoreDbFromLatestBackup(dbPathRestore, dbPathRestore, ropts);
      try (RocksDB db = RocksDB.open(options, dbPathRestore)) {
        assert ("value1".equals(new String(db.get("key1".getBytes()))));
        assert ("backup".equals(new String(db.get("test".getBytes()))));
      }
    } catch (final RocksDBException e) {
      System.out.format("[ERROR] caught the unexpected exception -- %s\n", e);
      assert (false);
    }
  }
}
