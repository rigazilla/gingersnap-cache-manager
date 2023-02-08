package io.gingersnapproject.database;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.infinispan.commons.dataconversion.internal.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.gingersnapproject.configuration.Rule;
import io.gingersnapproject.configuration.RuleEvents;
import io.gingersnapproject.configuration.RuleManager;
import io.gingersnapproject.database.model.Table;
import io.gingersnapproject.database.vendor.Vendor;
import io.quarkus.arc.Priority;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.PreparedQuery;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;

@ApplicationScoped
public class DatabaseHandler {
   private static final Logger log = LoggerFactory.getLogger(DatabaseHandler.class);
   @Inject
   RuleManager ruleManager;
   @Inject
   Pool pool;
   @Inject
   @ConfigProperty(name = "quarkus.datasource.db-kind")
   String dbKind;
   Vendor vendor;

   Map<String, Table> tables;
   Map<String, String> table2rule;

   private Vendor getVendor() {
      if (vendor == null) {
         log.debug("DB Kind is {}", dbKind);
         vendor = Vendor.fromDbKind(dbKind);
      }
      ;
      return vendor;
   }

   private Map<String, String> getTable2Rule() {
      // Lazy initialization for table2rule
      if (table2rule == null) {
         table2rule = ruleManager.eagerRules().entrySet().stream()
               .collect(Collectors.toMap(t -> t.getValue().connector().table(), Map.Entry::getKey));
         log.debug("Tables2rule read for static rules:");
         getTable2Rule().forEach((table, rule) -> log.debug("{}->{}", table, rule));
      }
      return table2rule;
   }

   private Map<String, Table> getTables() {
      // Lazy initialization for tables
      if (tables == null) {
         tables = ruleManager.eagerRules().values().stream()
               .map(rule -> getVendor().describeTable(pool, rule.connector().table()).await().indefinitely())
               .filter(Objects::nonNull)
               .collect(Collectors.toMap(Table::name, Function.identity()));
         log.debug("Tables read for static rules:");
         tables.forEach((name, table) -> log.debug("{}->{}", name, table.name()));
      }
      return tables;
   }

   public void addRuleEvent(@Observes @Priority(10) RuleEvents.EagerRuleAdded ev) {
      log.debug("Received events RuleEvents.EagerRuleAdded({}): {}", ev.name(), ev.rule());
      log.debug("vendor: {}", getVendor());
      var table = getVendor().describeTable(pool, ev.rule().connector().table()).await().indefinitely();
      if (table != null) {
         getTables().put(table.name(), table);
         getTable2Rule().put(ev.rule().connector().table(), ev.name());
      }
   }

   public void removeRuleEvent(@Observes RuleEvents.EagerRuleRemoved ev) {
      log.debug("Received events RuleEvents.EagerRuleRemoved({})", ev.name());
      getTable2Rule().remove(ev.name());
      getTables().remove(ev.name());
   }

   public Uni<String> select(Rule ruleConfig, String key) {
      // Have to do this until bug is fixed allowing injection of reactive Pool
      var query = prepareQuery(pool, ruleConfig.selectStatement());

      String[] arguments = ruleConfig.keyType().toArguments(key, ruleConfig.plainSeparator());
      return query
            .execute(Tuple.from(arguments))
            .onFailure().invoke(t -> log.error("Exception encountered!", t))
            .map(rs -> {
               if (rs.size() > 1) {
                  throw new IllegalArgumentException("Result set for " + ruleConfig.selectStatement()
                        + " for key: " + key + " returned " + rs.size() + " rows, it should only return 1");
               }
               int columns = rs.columnsNames().size();
               RowIterator<Row> rowIterator = rs.iterator();
               if (!rowIterator.hasNext()) {
                  return null;
               }
               Row row = rowIterator.next();
               Json jsonObject = Json.object();
               for (int i = 0; i < columns; ++i) {
                  jsonObject.set(row.getColumnName(i), row.getValue(i));
               }
               return jsonObject.toString();
            });
   }

   public static PreparedQuery<RowSet<Row>> prepareQuery(Pool pool, String sql) {
      io.vertx.sqlclient.PreparedQuery<RowSet<Row>> preparedQuery = pool.preparedQuery(sql);
      return io.vertx.mutiny.sqlclient.PreparedQuery.newInstance(preparedQuery);
   }

   public Table table(String name) {
      return getTables().get(name);
   }

   public String tableToRuleName(String name) {
      return getTable2Rule().get(name);
   }
}
