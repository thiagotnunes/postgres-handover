/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.handover;

import com.google.api.gax.paging.Page;
import com.google.cloud.spanner.Backup;
import com.google.cloud.spanner.BackupId;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Main {

  private static final String HOST = "staging-wrenchworks.sandbox.googleapis.com";
  private static final String URL = "https://" + HOST;
  private static final String PROJECT = "span-cloud-testing";
  private static final String INSTANCE = "spanner-testing";
  private static final String DATABASE = "thiagotnunes-pg-test1";

  public static void main(String[] args) throws Exception {
    final DatabaseId id = DatabaseId.of(PROJECT, INSTANCE, DATABASE);

    // -------
    // Standalone Client
    // On client creation we can manually set the dialect
    final SpannerOptions options = SpannerOptions
        .newBuilder()
        .setHost(URL)
        .setDialect(Dialect.POSTGRESQL)
        .build();
    try (Spanner spanner = options.getService()) {
      // Admin operations
      final DatabaseAdminClient databaseAdminClient = spanner.getDatabaseAdminClient();
      // createDatabase(id, databaseAdminClient);
      // createTable(id, databaseAdminClient);
      // createBackup(id, databaseAdminClient);
      getDatabase(id, databaseAdminClient);
      listDatabase(id, databaseAdminClient);
      System.out.println();

      // Data operations
      final DatabaseClient databaseClient = spanner.getDatabaseClient(id);
      deleteData(databaseClient);
      insertData(databaseClient);
      queryData(databaseClient);
      System.out.println();
    }



    // ------
    // JDBC
    // Data operations
    try (Connection connection = DriverManager
        .getConnection("jdbc:cloudspanner://" + HOST + "/" + id + "?dialect=postgresql")) {
      deleteData(connection);
      insertData(connection);
      queryData(connection);
    }
  }

  private static void createDatabase(DatabaseId id, DatabaseAdminClient databaseAdminClient)
      throws ExecutionException, InterruptedException, TimeoutException {
    final Database databaseToCreate = databaseAdminClient
        .newDatabaseBuilder(id)
        .setDialect(Dialect.POSTGRESQL)
        .build();
    final Database createdDatabase = databaseAdminClient
        .createDatabase(databaseToCreate, Collections.emptyList())
        .get(5, TimeUnit.MINUTES);
    System.out.println(
        "[Admin] Created database "
            + createdDatabase.getId()
            + " with dialect "
            + createdDatabase.getDialect()
    );
  }

  private static void createTable(DatabaseId id, DatabaseAdminClient databaseAdminClient)
      throws ExecutionException, InterruptedException, TimeoutException {
    // This could also had been created when the database was created
    databaseAdminClient
        .updateDatabaseDdl(
            id.getInstanceId().getInstance(),
            id.getDatabase(),
            Collections.singletonList("CREATE TABLE MyTable ("
                + "id BIGINT NOT NULL PRIMARY KEY,"
                + "numeric NUMERIC NOT NULL"
                + ")"),
            null
        ).get(5, TimeUnit.MINUTES);
    System.out.println("[Admin] Created table MyTable in " + id);
  }

  private static void createBackup(DatabaseId id, DatabaseAdminClient databaseAdminClient)
      throws ExecutionException, InterruptedException, TimeoutException {
    final Backup backupToCreate = databaseAdminClient
        .newBackupBuilder(BackupId.of(
            id.getInstanceId().getProject(),
            id.getInstanceId().getInstance(),
            id.getDatabase() + "-backup"
        ))
        .setDatabase(id)
        .build();

    final Backup createdBackup = databaseAdminClient
        .createBackup(backupToCreate)
        .get(30, TimeUnit.MINUTES);

    System.out.println(
        "[Admin] Created backup "
            + createdBackup.getId()
            + " for database "
            + createdBackup.getDatabase()
            + " with dialect "
            + createdBackup.getProto().getDatabaseDialect()
    );
  }

  private static void getDatabase(DatabaseId id, DatabaseAdminClient databaseAdminClient) {
    final Database retrievedDatabase = databaseAdminClient
        .getDatabase(id.getInstanceId().getInstance(), id.getDatabase());
    System.out.println(
        "Retrieved database "
            + retrievedDatabase.getId()
            + " with dialect "
            + retrievedDatabase.getDialect()
    );
  }

  private static void listDatabase(DatabaseId id, DatabaseAdminClient databaseAdminClient) {
    Page<Database> page = databaseAdminClient.listDatabases(INSTANCE);
    while (page != null) {
      for (Database database : page.getValues()) {
        if (database.getId().equals(id)) {
          System.out.println(
              "Listed database "
                  + database.getId()
                  + " with dialect "
                  + database.getDialect()
          );
        }
      }
      page = page.getNextPage();
    }
  }

  private static void dropDatabase(DatabaseId id, DatabaseAdminClient databaseAdminClient) {
    databaseAdminClient.dropDatabase(INSTANCE, DATABASE);
    System.out.println("Database " + id + " dropped");
  }

  private static void deleteData(DatabaseClient databaseClient) {
    databaseClient
        .readWriteTransaction()
        .run(transaction -> {
          transaction.executeUpdate(Statement.of("DELETE FROM MyTable WHERE TRUE"));
          return null;
        });

    System.out.println("[Standalone] Deleted data from MyTable");
  }

  private static void insertData(DatabaseClient databaseClient) {
    databaseClient
        .readWriteTransaction()
        .run(transaction -> {
          transaction.executeUpdate(Statement
              .newBuilder("INSERT INTO MyTable (id, numeric) VALUES ($1, $2)")
              .bind("p1")
              .to(1L)
              .bind("p2")
              .to(Value.pgNumeric("1.23"))
              .build()
          );

          transaction.buffer(Collections.singletonList(
              Mutation
                  .newInsertBuilder("MyTable")
                  .set("id")
                  .to(2L)
                  .set("numeric")
                  .to(Value.pgNumeric("NaN"))
                  .build())
          );

          return null;
        });
    System.out.println("[Standalone] Inserted data into MyTable");
  }

  private static void queryData(DatabaseClient databaseClient) {
    try (ResultSet resultSet = databaseClient.singleUse().executeQuery(Statement.of(
        "SELECT * FROM MyTable"
    ))) {
      while (resultSet.next()) {
        final long id = resultSet.getLong("id");
        final String numericAsString = resultSet.getString("numeric");
        final Value numericAsValue = resultSet.getValue("numeric");

        System.out.println("[Standalone] Retrieved row with id " + id);
        System.out.println("\tNumeric As String: " + numericAsString);
        System.out.println("\tNumeric As Value String: " + numericAsValue.getString());
        System.out.println("\tNumeric As Value Double: " + numericAsValue.getFloat64());
        try {
          System.out.println("\tNumeric As Value BigDecimal: " + numericAsValue.getNumeric());
        } catch (Exception e) {
          System.out.println("\tNumeric As Value BigDecimal: NaN (" + e.getMessage() + ")");
        }
      }
    }
  }

  private static void deleteData(Connection connection) throws SQLException {
    try (java.sql.Statement statement = connection.createStatement()) {
      statement.executeUpdate("DELETE FROM MyTable WHERE TRUE");
    }
    System.out.println("[JDBC] Deleted data from MyTable");
  }

  private static void insertData(Connection connection) throws SQLException {
    try (PreparedStatement preparedStatement = connection
        .prepareStatement("INSERT INTO MyTable (id, numeric) VALUES (1, ?), (2, ?)")) {
      preparedStatement.setBigDecimal(1, new BigDecimal("1.23"));
      preparedStatement.setObject(2, Value.pgNumeric("NaN"));

      preparedStatement.executeUpdate();
    }
    System.out.println("[JDBC] Inserted data into MyTable");
  }

  private static void queryData(Connection connection) throws SQLException {
    try (
        java.sql.Statement statement = connection.createStatement();
        java.sql.ResultSet resultSet = statement.executeQuery("SELECT * FROM MyTable")
    ) {
      while (resultSet.next()) {
        final long id = resultSet.getLong("id");
        final String numericAsString = resultSet.getString("numeric");
        final double numericAsDouble = resultSet.getDouble("numeric");
        final Value numericAsValue = resultSet.getObject("numeric", Value.class);

        System.out.println("[JDBC] Retrieved row with id " + id);
        System.out.println("\tNumeric As String: " + numericAsString);
        System.out.println("\tNumeric As Double: " + numericAsDouble);
        System.out.println("\tNumeric As Value String: " + numericAsValue.getString());
        System.out.println("\tNumeric As Value Double: " + numericAsValue.getFloat64());
        try {
          System.out.println("\tNumeric As Value BigDecimal: " + numericAsValue.getNumeric());
        } catch (Exception e) {
          System.out.println("\tNumeric As Value BigDecimal: NaN (" + e.getMessage() + ")");
        }
      }
    }
  }
}
