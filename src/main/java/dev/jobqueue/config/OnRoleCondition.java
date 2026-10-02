package dev.jobqueue.config;

import java.util.EnumSet;
import java.util.Set;
import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

class OnRoleCondition extends SpringBootCondition {

  @Override
  public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata md) {
    Role wanted = (Role) md.getAnnotationAttributes(ConditionalOnRole.class.getName()).get("value");
    Set<Role> roles =
        Binder.get(context.getEnvironment())
            .bind("jobqueue.roles", Bindable.setOf(Role.class))
            .orElseGet(() -> EnumSet.allOf(Role.class));
    if (roles.isEmpty()) {
      roles = EnumSet.allOf(Role.class);
    }
    return roles.contains(wanted)
        ? ConditionOutcome.match("role " + wanted + " enabled")
        : ConditionOutcome.noMatch("role " + wanted + " not enabled");
  }
}
