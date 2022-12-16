package io.gingersnapproject.configuration;
import io.smallrye.config.WithDefault;

public interface EagerRule  extends Rule {

      Connector connector();

      @WithDefault("false")
      boolean queryEnabled();

      @WithDefault("true")
      boolean expandEntity();

}
