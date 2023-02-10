package io.gingersnapproject.database.vendor;

import static io.gingersnapproject.database.DatabaseHandler.prepareQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.gingersnapproject.database.model.ForeignKey;
import io.gingersnapproject.database.model.PrimaryKey;
import io.gingersnapproject.database.model.Table;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;

public class PostgreSQLVendor implements Vendor {
   private static final Logger log = LoggerFactory.getLogger(PostgreSQLVendor.class);

   @Override
   public Uni<Table> describeTable(Pool pool, String table) {
      // Obtain the PK columns
      Uni<PrimaryKey> pkUni =
            prepareQuery(pool, "select constraint_name, column_name from information_schema.key_column_usage where table_name = '$1' and constraint_name in (select constraint_name from information_schema.table_constraints where table_name = '$2' and constraint_type='PRIMARY KEY') order by ordinal_position")
                  .execute(Tuple.of(table, table))
                  .onFailure().invoke(t -> log.error("Error retrieving primary key from " + table, t))
                  .map(Vendor::extractPrimaryKey);
      Uni<List<ForeignKey>> fksUni =
            prepareQuery(pool, "select kcu.constraint_name, kcu.column_name, ccu.table_name as ref_table, ccu.column_name as ref_column from information_schema.key_column_usage kcu, information_schema.constraint_column_usage ccu where kcu.constraint_name=ccu.constraint_name and kcu.table_name = '$1' and kcu.constraint_name in (select constraint_name from information_schema.table_constraints where table_name = '$2' and constraint_type='FOREIGN KEY') order by kcu.ordinal_position")
                  .execute(Tuple.of(table, table))
                  .onFailure().invoke(t -> log.error("Error retrieving foreign keys from " + table, t))
                  .map(rs -> {
                     List<ForeignKey> foreignKeys = new ArrayList<>(2);
                     String fkName = null;
                     List<String> columns = new ArrayList<>(2);
                     String fkTable = null;
                     List<String> fkColumns = new ArrayList<>(2);
                     for (RowIterator<Row> i = rs.iterator(); i.hasNext(); ) {
                        Row row = i.next();
                        String newFkName = row.getString(0);
                        if (fkName != null && !newFkName.equals(fkName)) {
                           foreignKeys.add(new ForeignKey(fkName, columns, fkTable, fkColumns));
                           columns = new ArrayList<>(2);
                           fkColumns = new ArrayList<>(2);
                        }
                        fkName = newFkName;
                        fkTable = row.getString(1);
                        columns.add(row.getString(2));
                        fkColumns.add(row.getString(3));
                     }
                     foreignKeys.add(new ForeignKey(fkName, columns, fkTable, fkColumns));
                     return foreignKeys;
                  });
      return Uni.combine().all().unis(pkUni, fksUni).combinedWith((pk, fks) -> new Table(table, pk, fks));
   }

   @Override
   public String whereClause(List<String> keys) {
      return IntStream.range(0, keys.size())
            .mapToObj(index -> keys.get(index) + " = $" + (index + 1))
            .collect(Collectors.joining(" AND "));
   };
}
