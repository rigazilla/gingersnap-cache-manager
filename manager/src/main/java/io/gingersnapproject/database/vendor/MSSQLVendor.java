package io.gingersnapproject.database.vendor;

import io.gingersnapproject.database.model.ForeignKey;
import io.gingersnapproject.database.model.PrimaryKey;
import io.gingersnapproject.database.model.Table;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.gingersnapproject.database.DatabaseHandler.prepareQuery;

public class MSSQLVendor implements Vendor {

    private static final Logger log = LoggerFactory.getLogger(MSSQLVendor.class);

    @Override
    public Uni<Table> describeTable(Pool pool, String table) {
        String pkSQL =
                "SELECT \n" +
                "     KU.table_name as TABLENAME\n" +
                "    ,column_name as PRIMARYKEYCOLUMN\n" +
                "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC \n" +
                "\n" +
                "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU\n" +
                "    ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' \n" +
                "    AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME \n" +
                "    AND KU.table_name = @P1\n" +
                "\n" +
                "ORDER BY \n" +
                "     KU.TABLE_NAME\n" +
                "    ,KU.ORDINAL_POSITION\n" +
                ";";
        Uni<PrimaryKey> pkUni =
                prepareQuery(pool, pkSQL)
                        .execute(Tuple.of(table))
                        .onFailure().invoke(t -> log.error("Error retrieving primary key from " + table, t))
                        .map(Vendor::extractPrimaryKey);

        String fkSQL =
                "SELECT\n" +
                "    Constraint_Name = C.CONSTRAINT_NAME,\n" +
                "    PK_Table = PK.TABLE_NAME,\n" +
                "    FK_Column = CU.COLUMN_NAME,\n" +
                "    PK_Column = PT.COLUMN_NAME\n" +
                "FROM\n" +
                "    INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS C\n" +
                "INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS FK\n" +
                "    ON C.CONSTRAINT_NAME = FK.CONSTRAINT_NAME\n" +
                "INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK\n" +
                "    ON C.UNIQUE_CONSTRAINT_NAME = PK.CONSTRAINT_NAME\n" +
                "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE CU\n" +
                "    ON C.CONSTRAINT_NAME = CU.CONSTRAINT_NAME\n" +
                "INNER JOIN (\n" +
                "            SELECT\n" +
                "                i1.TABLE_NAME,\n" +
                "                i2.COLUMN_NAME\n" +
                "            FROM\n" +
                "                INFORMATION_SCHEMA.TABLE_CONSTRAINTS i1\n" +
                "            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE i2\n" +
                "                ON i1.CONSTRAINT_NAME = i2.CONSTRAINT_NAME\n" +
                "            WHERE\n" +
                "                i1.CONSTRAINT_TYPE = 'PRIMARY KEY'\n" +
                "           ) PT\n" +
                "    ON PT.TABLE_NAME = @P1";

        Uni<List<ForeignKey>> fksUni =
                prepareQuery(pool, fkSQL)
                        .execute(Tuple.of(table))
                        .onFailure().invoke(t -> log.error("Error retrieving foreign keys from " + table, t))
                        .map(rs -> {
                            if (rs.size() == 0) {
                                return Collections.emptyList();
                            }
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

        return Uni.combine().all().unis(pkUni, fksUni).combinedWith((pk, fks) -> new Table(table, pk, fks)).onFailure().recoverWithNull();
    }

    @Override
    public String whereClause(List<String> keys) {
        return IntStream.range(0,keys.size())
            .mapToObj(index -> keys.get(index)+ " = @p"+ index)
            .collect(Collectors.joining(" AND "));
    };
}
