package dev.jobqueue.config;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.context.annotation.Conditional;

/** Registers the annotated bean only when {@code jobqueue.roles} contains the given role. */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
@Documented
@Conditional(OnRoleCondition.class)
public @interface ConditionalOnRole {
  Role value();
}
