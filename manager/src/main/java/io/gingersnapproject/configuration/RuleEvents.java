package io.gingersnapproject.configuration;

/** Collects all the rule related events */
public class RuleEvents {
   public record LazyRuleAdded(String name, LazyRule rule) {
   }

   public record LazyRuleRemoved(String name) {
   }

   public record EagerRuleAdded(String name, EagerRule rule) {
   }

   public record EagerRuleRemoved(String name) {
   }
}