package io.gingersnapproject.configuration;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Event;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class NotificationManager {
    static Logger log = LoggerFactory.getLogger(NotificationManager.class);
    @Inject
    Event<RuleEvents.LazyRuleAdded> lazyEventAdd;
    @Inject
    Event<RuleEvents.LazyRuleRemoved> lazyEventRemoved;
    @Inject
    Event<RuleEvents.EagerRuleAdded> eagerEventAdd;
    @Inject
    Event<RuleEvents.EagerRuleRemoved> eagerEventRemoved;
    
    public void lazyRuleAdded(String name, LazyRule rule) {
        log.debug("Firing RuleEvents.LazyRuleAdded({})", name);
        lazyEventAdd.fire(new RuleEvents.LazyRuleAdded(name, rule));
    }

    public void lazyRuleRemoved(String name) {
        log.debug("Firing RuleEvents.LazyRuleRemoved({})", name);
        lazyEventRemoved.fire(new RuleEvents.LazyRuleRemoved(name));
    }

    public void eagerRuleAdded(String name, EagerRule rule) {
        log.debug("Firing RuleEvents.EagerRuleAdded({})", name);
        eagerEventAdd.fire(new RuleEvents.EagerRuleAdded(name, rule));
    }

    public void eagerRuleRemoved(String name) {
        log.debug("Firing RuleEvents.EagerRuleRemoved({})", name);
        eagerEventRemoved.fire(new RuleEvents.EagerRuleRemoved(name));
    }
}
