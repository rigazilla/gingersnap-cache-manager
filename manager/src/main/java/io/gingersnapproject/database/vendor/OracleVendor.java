package io.gingersnapproject.database.vendor;

import static io.gingersnapproject.database.DatabaseHandler.prepareQuery;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.gingersnapproject.database.model.ForeignKey;
import io.gingersnapproject.database.model.PrimaryKey;
import io.gingersnapproject.database.model.Table;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;

/**
 * TODO!
 */
public class OracleVendor implements Vendor {

   private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final String PRIMARY_KEY_QUERY =
         "SELECT cols.constraint_name, cols.column_name" +
               " FROM all_constraints cons, all_cons_columns cols" +
               " WHERE cols.table_name = ?" +
               " AND cons.constraint_type = 'P'" +
               " AND cons.constraint_name = cols.constraint_name" +
               " AND cons.owner = cols.owner" +
               " ORDER BY cols.position";

   private static final String FOREIGN_KEY_QUERY =
         "WITH constraint_colum_list as" +
               " (SELECT owner, table_name, constraint_name, listagg(column_name,',') WITHIN GROUP ( ORDER BY position ) as column_list FROM all_cons_columns GROUP BY owner, table_name, constraint_name )" +
               " SELECT DISTINCT c1.constraint_name, c2.column_list, c3.table_name, c3.column_list" +
               " FROM all_constraints c1" +
               " JOIN constraint_colum_list c2 ON c1.constraint_name=c2.constraint_name and c1.owner=c2.owner" +
               " JOIN constraint_colum_list c3 ON c1.r_constraint_name=c3.constraint_name AND c1.r_owner=C3.owner" +
               " WHERE c1.constraint_type = 'R' and c1.table_name = ?";

   @Override
   public Uni<Table> describeTable(Pool pool, String table) {
      // Obtain the PK columns
      Uni<PrimaryKey> pkUni = prepareQuery(pool, PRIMARY_KEY_QUERY)
            .execute(Tuple.of(table.toUpperCase()))
            .onFailure().invoke(t -> log.error("Error retrieving primary key from " + table, t))
            .map(Vendor::extractPrimaryKeyInLowerCase);
      Uni<List<ForeignKey>> fksUni = prepareQuery(pool, FOREIGN_KEY_QUERY)
            .execute(Tuple.of(table.toUpperCase()))
            .onFailure().invoke(t -> log.error("Error retrieving foreign keys from " + table, t))
            .map(rs -> {
               List<ForeignKey> foreignKeys = new ArrayList<>(rs.size());
               for (Row row : rs) {
                  foreignKeys.add(foreignKeyFromRow(row));
               }
               return foreignKeys;
            });
      return Uni.combine().all().unis(pkUni, fksUni).combinedWith((pk, fks) -> new Table(table.toLowerCase(), pk, fks));
   }

   private static ForeignKey foreignKeyFromRow(Row row) {
      return new ForeignKey(row.getString(0).toLowerCase(),
            splitColumnList(row.getString(1).toLowerCase()),
            row.getString(2).toLowerCase(),
            splitColumnList(row.getString(3).toLowerCase()));
   }

   private static List<String> splitColumnList(String columns) {
      return Arrays.asList(columns.split(","));
   }

   @Override
   public String whereClause(List<String> keys) {
      return keys.stream().map(s -> s + " = ?").collect(Collectors.joining(" AND "));
   }
}
