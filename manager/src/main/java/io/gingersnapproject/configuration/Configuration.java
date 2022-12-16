package io.gingersnapproject.configuration;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
@ConfigMapping(prefix = "gingersnap")
public interface Configuration {
   static final Logger log = LoggerFactory.getLogger(Configuration.class);

   HotRod hotrod();

   @WithName("lazy-rule")
   @WithDefault("")
   Map<String, LazyRule> lazyRules();
   
   @WithName("eager-rule")
   @WithDefault("")
   Map<String, EagerRule> eagerRules();
}
