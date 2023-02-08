package io.gingersnapproject.k8s;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.gingersnapproject.configuration.Connector;
import io.gingersnapproject.configuration.EagerRule;
import io.gingersnapproject.configuration.LazyRule;
import io.gingersnapproject.configuration.RuleManager;
import io.gingersnapproject.database.vendor.Vendor;
import io.gingersnapproject.k8s.configuration.KubernetesConfiguration;
import io.gingersnapproject.proto.api.config.v1alpha1.EagerCacheRuleSpec;
import io.gingersnapproject.proto.api.config.v1alpha1.LazyCacheRuleSpec;
import io.quarkus.arc.Priority;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Creates two informer on k8s configMap for Lazy and Eager Gingersnap rules
 * ConfigMap names come from properties
 * gingersnap.k8s.eager-config-map
 * gingersnap.k8s.lazy-config-map
 */
@ApplicationScoped
@RegisterForReflection(targets = { io.gingersnapproject.proto.api.config.v1alpha1.KeyFormat.class })
public class CacheRuleInformer {
   @Inject
   Instance<KubernetesClient> client;
   @Inject
   KubernetesConfiguration configuration;
   @Inject
   RuleManager rManager;

   private SharedIndexInformer<ConfigMap> lazyInformer;
   private SharedIndexInformer<ConfigMap> eagerInformer;
   private static String dbKind;

   private static final Logger log = LoggerFactory.getLogger(CacheRuleInformer.class);

   void startWatching(@Observes StartupEvent ignore, Config config) {
      log.debug("startWatching(): begin");
      dbKind = config.getValue("quarkus.datasource.db-kind", String.class).toLowerCase();
      if (client.isUnsatisfied()) {
         log.error("Kubernetes client not found, not watching config map");
         return;
      }
      try {
         var cmName = configuration.lazyConfigMapName();
         if (cmName.isEmpty()) {
            throw new RuntimeException("Lazy configMap not specified, not watching config map");
         }
         var leh = new LazyEventHandler(rManager);
         lazyInformer = configureInformer(cmName.get(), leh);
         lazyInformer.start();
      } catch (RuntimeException ex) {
         log.error("Lazy rules informer startup failed", ex);
      }

      try {
         var cmName = configuration.eagerConfigMapName();
         if (cmName.isEmpty()) {
            throw new RuntimeException("Eager configMap not specified, not watching config map");
         }
         var eeh = new EagerEventHandler(rManager);
         eagerInformer = configureInformer(cmName.get(), eeh);
         eagerInformer.start();
      } catch (RuntimeException ex) {
         log.error("Eager rules informer startup failed", ex);
      }
   }

   public void stop(@Observes ShutdownEvent ignore) {
      if (lazyInformer != null) {
         log.debug("Shutdown lazyInformer");
         lazyInformer.close();
      }

      if (eagerInformer != null) {
         log.debug("Shutdown eagerInformer");
         eagerInformer.close();
      }
   }

   private SharedIndexInformer<ConfigMap> configureInformer(
         String configMapName,
         ConfigMapEventHandler handler) {

      log.debug("Informer on configMap {}", configMapName);
      var RESYNC_PERIOD = 60 * 1000L;
      KubernetesClient kc = client.get();
      Resource<ConfigMap> cm = kc.configMaps().withName(configMapName);

      return cm.inform(handler, RESYNC_PERIOD);
   }

   /**
    * Knows how to hadle k8s events from informer
    * Generates add and remove rules events for the cache-manager application
    */
   static abstract class ConfigMapEventHandler implements ResourceEventHandler<ConfigMap> {
      /** The dispatcher, it could be for Lazy or Eager rule */
      private static final Logger log = LoggerFactory.getLogger(ConfigMapEventHandler.class);
      RuleManager rManager;

      ConfigMapEventHandler(RuleManager r) {
         rManager = r;
      }

      abstract void addRule(String k, String v);

      abstract void removeRule(String k, String v);

      private void processConfigMapAndSend(Set<Map.Entry<String, String>> entries,
            BiConsumer<String, String> sendEventFunc) {
         for (Entry<String, String> entry : entries) {
            sendEventFunc.accept(entry.getKey(), entry.getValue());
         }
      }

      @Override
      public void onAdd(ConfigMap obj) {
         log.debug("calling onAdd for ConfigMap {}", obj.getMetadata().getName());
         processConfigMapAndSend(obj.getData().entrySet(), (k, v) -> addRule(k, v));
      }

      @Override
      public void onUpdate(ConfigMap oldObj, ConfigMap newObj) {
         Map<String, String> oldMap = oldObj.getData();
         Map<String, String> newMap = newObj.getData();
         var olds = oldMap.entrySet();
         var news = newMap.entrySet();

         checkNoUpdatesOrThrow(oldMap, newMap, olds, news);

         var added = new HashSet<>(news);
         // Get added keys. From new set remove
         // - keys already present in old set
         added.removeIf(arg0 -> oldMap.containsKey(arg0.getKey()));

         var removed = new HashSet<>(olds);
         // Get removed keys. From old set remove
         // - keys still present in new set
         removed.removeIf((arg0 -> newMap.containsKey(arg0.getKey())));

         processConfigMapAndSend(removed, (name, rule) -> removeRule(name, rule));
         processConfigMapAndSend(added, (name, rule) -> addRule(name, rule));
      }

      /** Checks that now updates are requested */
      private void checkNoUpdatesOrThrow(Map<String, String> oldMap, Map<String, String> newMap,
            Set<Entry<String, String>> olds,
            Set<Entry<String, String>> news) {
         var changedNewValues = new HashSet<>(news);
         // Get the changed keys. From the new set remove
         // - keys which aren't in the old set (added)
         // - keys which are in the new set with same value (unchanged)
         changedNewValues.removeIf(
               arg0 -> !oldMap.containsKey(arg0.getKey()) || arg0.getValue().equals(oldMap.get(arg0.getKey())));
         if (changedNewValues.size() > 0) {
            // Update rule is unsupported and this should never happens. Assuption here is
            // that the configMap
            // is corrupted, so we raise an exception and no events are sent to the rule
            // manager. Exception contains rule with old and new values
            var changedOldValues = new HashSet<>(olds);
            changedOldValues.removeIf(
                  arg0 -> !newMap.containsKey(arg0.getKey()) || arg0.getValue().equals(newMap.get(arg0.getKey())));
            throw new UnsupportedOperationException("Rules cannot be updated: new values " + changedNewValues
                  + ", old values " + changedOldValues);
         }
      }

      @Override
      public void onDelete(ConfigMap obj, boolean deletedFinalStateUnknown) {
         processConfigMapAndSend(obj.getData().entrySet(), (name, rule) -> removeRule(name, rule));
      }
   }

   static class LazyEventHandler extends ConfigMapEventHandler {
      LazyEventHandler(RuleManager r) {
         super(r);
      }

      @Override
      public void addRule(String key, String value) {
         log.debug("Adding LazyRule({})", key);
         var rule = parse(value);
         rManager.addLazyRule(key, new LazyCacheRuleSpecAdapter(rule.build()));
      }

      @Override
      public void removeRule(String key, String value) {
         log.debug("Removing LazyRule({})", key);
         rManager.removeLazyRule(key);
      }

      private LazyCacheRuleSpec.Builder parse(String s) {
         var eRuleBuilder = LazyCacheRuleSpec.newBuilder();
         try {
            JsonFormat.parser().ignoringUnknownFields().merge(s, eRuleBuilder);
         } catch (InvalidProtocolBufferException e) {
            log.error("Cannot parse eager rule with name {}", e);
         }
         return eRuleBuilder;
      }
   }

   static class EagerEventHandler extends ConfigMapEventHandler {
      EagerEventHandler(RuleManager r) {
         super(r);
      }

      @Override
      public void addRule(String key, String value) {
         log.debug("Adding EagerRule({})", key);
         var rule = parse(value);
         rManager.addEagerRule(key, new EagerCacheRuleSpecAdapter(rule.build()));
      }

      @Override
      public void removeRule(String key, String value) {
         log.debug("Removing EagerRule({})", key);
         rManager.removeEagerRule(key);
      }

      private EagerCacheRuleSpec.Builder parse(String s) {
         var eRuleBuilder = EagerCacheRuleSpec.newBuilder();
         try {
            JsonFormat.parser().ignoringUnknownFields().merge(s, eRuleBuilder);
         } catch (InvalidProtocolBufferException e) {
            log.error("Cannot parse eager rule with name {}", e);
         }
         return eRuleBuilder;
      }
   }

   /** Adapt an LazyCacheRuleSpec to be and LazyRule */
   static class LazyCacheRuleSpecAdapter implements LazyRule {
      final LazyCacheRuleSpec lazyRule;

      public LazyCacheRuleSpecAdapter(LazyCacheRuleSpec LazyRule) {
         this.lazyRule = LazyRule;
      }

      @Override
      public KeyType keyType() {
         switch (lazyRule.getKey().getFormat()) {
            case TEXT:
               return KeyType.TEXT;
            case JSON:
               return KeyType.JSON;
            case UNRECOGNIZED:
               throw new IllegalArgumentException("Unsupported KeyType");
         }
         throw new IllegalArgumentException("Unsupported KeyType");
      }

      @Override
      public String plainSeparator() {
         return lazyRule.getKey().getKeySeparator();
      }

      @Override
      public String selectStatement() {
         return lazyRule.getQuery();
      }

      @Override
      public String toString() {
         return "LazyCacheRuleSpecAdapter{" +
               "keyType=" + keyType() +
               ",plainSeparator=" + plainSeparator() +
               ",selectStatement=" + selectStatement() +
               '}';
      }
   }

   /** Adapt an EagerCacheRuleSpec to be and EagerRule */
   static class EagerCacheRuleSpecAdapter implements EagerRule {
      final EagerCacheRuleSpec eagerRule;
      private String selectStmt;
      private Connector connector;

      public EagerCacheRuleSpecAdapter(EagerCacheRuleSpec eagerRule) {
         this.eagerRule = eagerRule;
      }

      @Override
      public Connector connector() {
         if (connector == null) {
            String[] schemaTable = eagerRule.getTableName().split("\\.");
            if (schemaTable.length > 2) {
               throw new IllegalArgumentException("Unsupported schema format (must be table or schema.table)");
            }
            connector = new Connector() {
               @Override
               public String schema() {
                  return (schemaTable.length == 1) ? "" : schemaTable[0];
               }

               @Override
               public String table() {
                  return (schemaTable.length == 1) ? schemaTable[0] : schemaTable[1];
               }
            };
         }
         return connector;
      }

      @Override
      public KeyType keyType() {
         return switch (eagerRule.getKey().getFormat()) {
            case TEXT -> KeyType.TEXT;
            case JSON -> KeyType.JSON;
            case UNRECOGNIZED -> throw new IllegalArgumentException("Unsupported KeyType");
         };
      }

      @Override
      public String plainSeparator() {
         return eagerRule.getKey().getKeySeparator();
      }

      @Override
      public String selectStatement() {
         if (selectStmt == null) {
            buildStatement();
         }
         return selectStmt;
      }

      @Override
      public boolean queryEnabled() {
         return eagerRule.getQuery().getEnabled();
      }

      @Override
      public boolean expandEntity() {
         return true;
      }

      @Override
      public String toString() {
         return "EagerCacheRuleSpecAdapter{" +
               "connector.table=" + (connector() == null ? null : connector().table()) +
               ",connector.schema=" + (connector() == null ? null : connector().schema()) +
               ",keyType=" + keyType() +
               ",plainSeparator=" + plainSeparator() +
               ",selectStatement=" + selectStatement() +
               ",queryEnabled=" + queryEnabled() +
               ",expandEntity=" + expandEntity() +
               '}';
      }

      private void buildStatement() {
         StringBuilder sb = new StringBuilder();
         sb.append("SELECT ")
               .append(
                     eagerRule.getValue().getValueColumnsCount() > 0
                           ? String.join(", ", eagerRule.getValue().getValueColumnsList())
                           : "*")
               .append(" FROM ")
               .append(eagerRule.getTableName());
         if (eagerRule.getKey().getKeyColumnsCount() > 0) {
            sb.append(" WHERE ").append(
               Vendor.fromDbKind(dbKind).whereClause(eagerRule.getKey().getKeyColumnsList()));
         }
         selectStmt = sb.toString();
      }

   }
}
