package io.gingersnapproject.database.vendor;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import io.gingersnapproject.database.model.PrimaryKey;
import io.gingersnapproject.database.model.Table;
import io.smallrye.mutiny.Uni;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

/**
 * @since 15.0
 **/
public interface Vendor {
   static Vendor fromDbKind(String dbKind) {
      return switch (dbKind.toLowerCase()) {
         case "postgresql":
         case "pgsql":
         case "pg":
            yield new PostgreSQLVendor();
         case "mssql":
            yield new MSSQLVendor();
         case "mysql":
         case "mariadb":
            yield new MySQLVendor();
         case "oracle":
            yield new OracleVendor();
         default:
            throw new UnsupportedOperationException();
      };
   }

   Uni<Table> describeTable(Pool pool, String table);

   static PrimaryKey extractPrimaryKey(RowSet<Row> rows) {
      String pkName = null;
      List<String> pkColumns = new ArrayList<>(2);
      for (Row row : rows) {
         pkName = row.getString(0);
         pkColumns.add(row.getString(1));
      }
      return new PrimaryKey(pkName, pkColumns);
   }

   static PrimaryKey extractPrimaryKeyInLowerCase(RowSet<Row> rows) {
      String pkName = null;
      List<String> pkColumns = new ArrayList<>(2);
      for (Row row : rows) {
         pkName = row.getString(0).toLowerCase();
         pkColumns.add(row.getString(1).toLowerCase());
      }
      return new PrimaryKey(pkName, pkColumns);
   }

   String whereClause(List<String> keys);
}
