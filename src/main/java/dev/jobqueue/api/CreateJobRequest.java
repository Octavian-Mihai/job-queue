package dev.jobqueue.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.time.OffsetDateTime;

public record CreateJobRequest(
    @NotBlank @Size(max = 100) String type,
    @Schema(description = "Arbitrary JSON passed to the handler; defaults to {}") JsonNode payload,
    @Size(min = 1, max = 100) @Schema(description = "Defaults to 'default'") String queue,
    @Min(-1000) @Max(1000) @Schema(description = "Higher runs first; default 0") Integer priority,
    @Min(0) @Max(31_536_000) @Schema(description = "Run no earlier than now + this many seconds")
        Long delaySeconds,
    @Schema(
            description =
                "Run no earlier than this instant (ISO-8601). Exclusive with delaySeconds")
        OffsetDateTime runAt,
    @Min(1) @Max(100) @Schema(description = "Total attempts before dead-lettering; default 5")
        Integer maxAttempts) {

  @JsonIgnore
  @AssertTrue(message = "delaySeconds and runAt are mutually exclusive")
  public boolean isScheduleValid() {
    return delaySeconds == null || runAt == null;
  }
}
