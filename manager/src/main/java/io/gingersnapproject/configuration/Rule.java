package io.gingersnapproject.configuration;

import io.smallrye.config.WithDefault;
import org.infinispan.commons.dataconversion.internal.Json;

public interface Rule {
    @WithDefault("|")
    String plainSeparator();

    String selectStatement();

    @WithDefault("TEXT")
    KeyType keyType();

    enum KeyType {
        TEXT {
              @Override
              public String[] toArguments(String strValue, String plainSeparator) {
                    return strValue.split(plainSeparator);
              }
        },
        JSON {
              @Override
              public String[] toArguments(String strValue, String plainSeparator) {
                    return Json.read(strValue)
                          .asJsonMap()
                          .values()
                          .stream()
                          .map(Json::toString)
                          .toArray(String[]::new);
              }
        };
        public abstract String[] toArguments(String strValue, String plainSeparator);
  }

}
