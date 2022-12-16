package io.gingersnapproject.configuration;

import java.util.HashMap;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.config.SmallRyeConfig;

@ApplicationScoped
public class RuleManager {

   SmallRyeConfig smConfig = ConfigProvider.getConfig().unwrap(SmallRyeConfig.class);
   Configuration config = smConfig.getConfigMapping(Configuration.class);

   Map<String, EagerRule> runtimeEagerRules = new HashMap<>();
   Map<String, LazyRule> runtimeLazyRules = new HashMap<>();

   static final Logger log = LoggerFactory.getLogger(RuleManager.class);

   @Inject
   NotificationManager nManager;

   public Map<String, EagerRule> eagerRules() {
      return runtimeEagerRules;
   }

   public Map<String, LazyRule> lazyRules() {
      return runtimeLazyRules;
   }

   // TODO: This method should disappear when different APIs will be provided
   // for Eager and Lazy rules
   public Rule getEagerOrLazyRuleByName(String name) {
      if (eagerRules().containsKey(name)) {
         return eagerRules().get(name);
      }
      return lazyRules().get(name);
   }

   // TODO: This method should disappear when different APIs will be provided
   // for Eager and Lazy rules
   public boolean containsRuleByName(String name) {
      return eagerRules().containsKey(name) || lazyRules().containsKey(name);
   }

   public RuleManager() {
      log.debug("Instantiating RuleManager");
      log.debug("Reading static rules from configuration");
      runtimeEagerRules.putAll(config.eagerRules());
      runtimeLazyRules.putAll(config.lazyRules());
      log.debug("Eager rules read:");
      runtimeEagerRules.keySet().forEach((name)-> log.debug("{} ",name));
      log.debug("Lazy rules read:");
      runtimeLazyRules.keySet().forEach((name)-> log.debug("{} ",name));
   }

   public void addLazyRule(String name, LazyRule rule) {
      log.debug("Adding LazyRule({}): {}", name, rule);
      addRule(runtimeLazyRules, name, rule);
      nManager.lazyRuleAdded(name, rule);
   }

   public void removeLazyRule(String name) {
      log.debug("Removing LazyRule({})", name);
      removeRule(runtimeLazyRules, name);
      nManager.lazyRuleRemoved(name);
   }

   public void addEagerRule(String name, EagerRule rule) {
      log.debug("Adding EagerRule({}): {}", name, rule);
      addRule(runtimeEagerRules, name, rule);
      nManager.eagerRuleAdded(name, rule);
   }

   public void removeEagerRule(String name) {
      log.debug("Removing EagerRule({})", name);
      removeRule(runtimeEagerRules, name);
      nManager.eagerRuleRemoved(name);
   }

   public <T> void removeRule(Map<String, T> map, String name) {
      var removed = map.remove(name);
      if (removed == null) {
         log.warn("Rule(" + name + ") not present. Not removed.");
      }
   }

   public <T> void addRule(Map<String, T> map, String name, T rule) {
      var ruleInMap = map.putIfAbsent(name, rule);
      if (ruleInMap != null) {
         throw new UnsupportedOperationException("Existing Rule(" + name + ") can't be updated.");
      }
   }
}
