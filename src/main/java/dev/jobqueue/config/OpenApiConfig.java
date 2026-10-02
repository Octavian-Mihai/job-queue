package dev.jobqueue.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class OpenApiConfig {

  @Bean
  OpenAPI jobQueueApi() {
    return new OpenAPI()
        .info(
            new Info()
                .title("Durable Job Queue API")
                .version("0.1.0")
                .description(
                    "Submit, inspect, cancel and replay background jobs. Delivery is at-least-once:"
                        + " use an Idempotency-Key to deduplicate submissions, and write handlers"
                        + " to be idempotent."));
  }
}
