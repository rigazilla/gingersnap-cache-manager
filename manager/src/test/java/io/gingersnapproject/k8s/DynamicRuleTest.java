package io.gingersnapproject.k8s;

import static io.restassured.RestAssured.given;

import javax.inject.Inject;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import io.gingersnapproject.configuration.RuleManager;
import io.gingersnapproject.k8s.CacheRuleInformer.EagerEventHandler;
import io.gingersnapproject.k8s.CacheRuleInformer.LazyEventHandler;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
@TestInstance(Lifecycle.PER_CLASS)
public class DynamicRuleTest {

  @Inject
  RuleManager rm;
  EagerEventHandler eHandler;
  LazyEventHandler lHandler;

  private static String eagerRuleA = """
      {
         "cacheRef": {
           "name": "myCache",
           "namespace": "myNamespace"
         },
         "tableName": "TABLE_EAGER_RULE_1",
         "key": {
           "format": "JSON",
           "keySeparator": ",",
           "keyColumns": ["col1", "col3", "col4"]
         },
         "value": {
           "valueColumns": ["col6", "col7", "col8"]
         }
       }
         """;
  private static String lazyRuleA = """
      {
         "cacheRef": {
           "name": "myCache",
           "namespace": "myNamespace"
         },
         "query": "select col0, col3, col5 from table_lazy_rule_A where cola = ? and colb = ? and colc = ?",
         "key": {
           "format": "JSON",
           "keySeparator": ",",
           "keyColumns": ["cola", "colb", "colc"]
         }
       }
         """;

  @BeforeAll
  public void beforeAll() {
    eHandler = new EagerEventHandler(rm);
    lHandler = new LazyEventHandler(rm);
  }

  @Test
  public void testAddRemoveEagerRule() throws Exception {
    eHandler.addRule("eagerRuleA", eagerRuleA);
    given()
        .when().get("/rules/eagerRuleA")
        .then()
        .assertThat().statusCode(200);
    eHandler.removeRule("eagerRuleA", eagerRuleA);
    Thread.sleep(2000, 0);
    given()
        .when().get("/rules/eagerRuleA")
        .then()
        .assertThat().statusCode(500);
  }

  @Test
  public void testAddRemoveLazyRule() throws Exception {
    lHandler.addRule("lazyRuleA", lazyRuleA);
    given()
        .when().get("/rules/lazyRuleA")
        .then()
        .assertThat().statusCode(200);
    lHandler.removeRule("lazyRuleA", lazyRuleA);
    Thread.sleep(2000, 0);
    given()
        .when().get("/rules/lazyRuleA")
        .then()
        .assertThat().statusCode(500);
  }
}
