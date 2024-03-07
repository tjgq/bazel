// Copyright 2024 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.google.devtools.build.lib.remote.disk;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.flogger.GoogleLogger;
import com.google.devtools.build.lib.concurrent.AbstractQueueVisitor;
import com.google.devtools.build.lib.concurrent.ErrorClassifier;
import com.google.devtools.build.lib.concurrent.NamedForkJoinPool;
import com.google.devtools.build.lib.profiler.Profiler;
import com.google.devtools.build.lib.profiler.SilentCloseable;
import com.google.devtools.build.lib.vfs.Dirent;
import com.google.devtools.build.lib.vfs.FileStatus;
import com.google.devtools.build.lib.vfs.IORuntimeException;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.Symlinks;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;

public class GarbageCollector {

  // TODO: collect to a low watermark
  // TODO: think about IOExceptions while updating database
  // TODO: temporary files also contribute to cache size
  // TODO: avoid lock on sql connection
  // TODO: oportunistically detect cache corruption and reindex

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private final Path root;

  private final long maxSize;

  @Nullable
  private Connection conn;

  private final Lock lock = new ReentrantLock();

  private static final ForkJoinPool VISITOR_POOL =
      NamedForkJoinPool.newNamedPool(
          "disk-cache-visitor", 32);

  private class Visitor extends AbstractQueueVisitor {

    private AtomicLong totalSize = new AtomicLong();

    Visitor(Path root, Connection conn) {
      super(
          VISITOR_POOL,
          ExecutorOwnership.SHARED,
          ExceptionHandlingMode.FAIL_FAST,
          ErrorClassifier.DEFAULT);
    }

    long run() throws IOException, InterruptedException {
      execute(
          () -> visitDirectory(root));
      try {
        awaitQuiescence(true);
      } catch (IORuntimeException e) {
        throw e.getCauseIOException();
      }
      return totalSize.get();
    }

    private void visitDirectory(Path path) {
      try (PreparedStatement stmt = conn.prepareStatement(
          "INSERT INTO metadata (name, mtime, size) VALUES (?, ?, ?)")) {
        for (Dirent dirent : path.readdir(Symlinks.NOFOLLOW)) {
          Path childPath = path.getChild(dirent.getName());
          if (dirent.getType().equals(Dirent.Type.FILE)) {
            visitFile(stmt, childPath);
          } else if (dirent.getType().equals(Dirent.Type.DIRECTORY)) {
            execute(() -> visitDirectory(childPath));
          }
        }
        stmt.executeBatch();
      } catch (IOException e) {
        throw new IORuntimeException(e);
      } catch (SQLException e) {
        throw new IORuntimeException(new IOException(e));
      }
    }

    private void visitFile(PreparedStatement stmt, Path path) throws IOException {
      try {
        FileStatus status = path.stat();
        stmt.setString(1, path.relativeTo(root).toString());
        stmt.setLong(2, status.getLastModifiedTime());
        stmt.setLong(3, status.getSize());
        stmt.addBatch();
        totalSize.addAndGet(status.getSize());
      } catch (SQLException e) {
        throw new IOException(e);
      }
    }
  }

  GarbageCollector(Path root, long maxSize) throws IOException {
    checkArgument(maxSize > 0);

    this.root = root;
    this.maxSize = maxSize;
    this.conn = openConnection();

    maybeRecreate();
  }

  void shutdown() throws IOException {
    checkNotNull(conn);
    try {
      lock.lock();  // TODO: interruptibly
      conn.close();
    } catch (SQLException e) {
      throw new IOException(e);
    } finally {
      lock.unlock();
    }
    conn = null;
  }

  private Connection openConnection() throws IOException {
    try {
      Connection conn = DriverManager.getConnection(
          String.format("jdbc:sqlite:%s?journal_mode=WAL&synchronous=NORMAL",
              root.getChild("index")));
      conn.setAutoCommit(false);
      return conn;
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  private void maybeRecreate() throws IOException {
    checkNotNull(conn);
    try (SilentCloseable c = Profiler.instance().profile("DiskCacheGC/maybeReindex")) {
      conn.setSavepoint();

      conn.createStatement().executeUpdate(
          "CREATE TABLE IF NOT EXISTS metadata (name TEXT PRIMARY KEY, mtime INTEGER, size INTEGER); "
              +
              "CREATE INDEX IF NOT EXISTS mtime_index ON metadata (mtime); " +
              "CREATE TABLE IF NOT EXISTS stats (dummy_key INTEGER PRIMARY KEY, last_created INTEGER, last_updated INTEGER, total_size INTEGER); ");

      boolean recreate;
      try (ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM stats")) {
        recreate = !rs.next(); // result set is empty
      }

      if (recreate) {
        long totalSize;
        try {
          totalSize = new Visitor(root, conn).run();
        } catch (InterruptedException e) {
          // TODO: handle elsewhere
          throw new IOException(e);
        }
        long currentTime = System.currentTimeMillis();

        try (PreparedStatement stmt = conn.prepareStatement(
            "INSERT INTO stats (dummy_key, last_created, last_updated, total_size) VALUES (0, ?, ?, ?)")) {
          stmt.setLong(1, currentTime);
          stmt.setLong(2, currentTime);
          stmt.setLong(3, totalSize);
          stmt.executeUpdate();
        }
      }

      conn.commit();

      /*conn.setAutoCommit(true);
      conn.createStatement().executeUpdate("VACUUM;");*/
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  void updateEntry(Path path, long timestamp, long size, boolean newlyAdded) throws IOException {
    try (SilentCloseable c = Profiler.instance().profile("DiskCacheGC/updateEntry")) {
      lock.lock();  // TODO: interruptibly
      conn.setSavepoint();

      if (newlyAdded) {
        do {
          long totalSize;
          try (ResultSet rs = conn.createStatement().executeQuery("SELECT total_size FROM stats")) {
            checkState(rs.next());
            totalSize = rs.getLong(1);
          }

          // TODO: underflow
          if (totalSize < maxSize - size) {
            break;
          }

          String nameToDelete;
          long sizeToDelete;
          try (ResultSet rs = conn.createStatement()
              .executeQuery("SELECT name, size FROM metadata ORDER BY mtime ASC LIMIT 1")) {
            checkState(rs.next());
            nameToDelete = rs.getString(1);
            sizeToDelete = rs.getLong(2);
          }

          try {
            root.getRelative(nameToDelete).delete();
          } catch (FileNotFoundException e) {
            // Intentionally ignored.
          }

          try (PreparedStatement stmt = conn.prepareStatement(
              "DELETE FROM metadata WHERE name = ?; UPDATE stats SET total_size = ?")) {
            stmt.setString(1, nameToDelete);
            stmt.setLong(2, totalSize - sizeToDelete);
            stmt.executeUpdate();
          }
        } while (true);
      }

      try (PreparedStatement stmt = conn.prepareStatement(
          "INSERT OR REPLACE INTO metadata VALUES (?, ?, ?)")) {
        stmt.setString(1, path.relativeTo(root).toString());
        stmt.setLong(2, timestamp);
        stmt.setLong(3, size);
        stmt.executeUpdate();
      }
      try (PreparedStatement stmt = conn.prepareStatement("UPDATE stats SET last_updated = ?")) {
        stmt.setLong(1, timestamp);
        stmt.executeUpdate();
      }
      conn.commit();
    } catch (SQLException e) {
      throw new IOException(e);
    } finally {
      lock.unlock();
    }
  }
}