package de.turing85.quarkus.camel.direct.attachment;

import java.time.Duration;
import java.util.Objects;

import jakarta.activation.DataHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.jms.ConnectionFactory;

import io.quarkus.logging.Log;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.camel.attachment.AttachmentMessage;
import org.apache.camel.builder.RouteBuilder;

import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.direct;
import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.jms;
import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.timer;

@ApplicationScoped
@AllArgsConstructor
@Getter(AccessLevel.PRIVATE)
public class JmsRoute extends RouteBuilder {
  private static final String ROUTE_ID_TIMER = "timer";
  private static final String ROUTE_ID_JMS_IN = "jms-in";
  private static final String ROUTE_ID_DOWNSTREAM_PROCESSING = "downstream-processing";

  private static final String QUEUE_IN = "queue:in";

  private final ConnectionFactory connectionFactory;

  @Override
  public void configure() {
    // @formatter:off
    from(
        timer(ROUTE_ID_TIMER)
            .delay(Duration.ofSeconds(1).toMillis())
            .period(Duration.ofMinutes(1).toMillis()))
        .id(ROUTE_ID_TIMER)
        .to(jms(QUEUE_IN)
            .connectionFactory(connectionFactory()));

    from(
        jms(QUEUE_IN)
            .connectionFactory(connectionFactory()))
        .routeId(ROUTE_ID_JMS_IN)
        .process(exchange -> {
          final AttachmentMessage attachmentMessage =
              Objects.requireNonNull(exchange.getMessage(AttachmentMessage.class));
          attachmentMessage.addAttachment("text.txt", new DataHandler("Hello!", "text/plain"));
          Log.infof(
              "In %s, before calling multicast: Attachments: %s",
              ROUTE_ID_JMS_IN,
              attachmentMessage.getAttachmentNames());
        })
        .multicast().stopOnException().shareUnitOfWork().synchronous()
          .to(direct(ROUTE_ID_DOWNSTREAM_PROCESSING))
        .end()
        .process(exchange -> {
          final AttachmentMessage attachmentMessage =
              Objects.requireNonNull(exchange.getMessage(AttachmentMessage.class));
          Log.infof(
              "In %s, after calling multicast: Attachments: %s",
              ROUTE_ID_JMS_IN,
              attachmentMessage.getAttachmentNames());
        });

    from(direct(ROUTE_ID_DOWNSTREAM_PROCESSING))
        .id(ROUTE_ID_DOWNSTREAM_PROCESSING)
        .process(exchange -> {
          final AttachmentMessage attachmentMessage =
              Objects.requireNonNull(exchange.getMessage(AttachmentMessage.class));
          Log.infof(
              "In %s, before adding new attachment: Attachments: %s",
              ROUTE_ID_DOWNSTREAM_PROCESSING,
              attachmentMessage.getAttachmentNames());
          attachmentMessage
              .addAttachment("text2.txt", new DataHandler("Hello number 2!", "text/plain"));
          Log.infof(
              "In %s, after adding new attachment: Attachments: %s",
              ROUTE_ID_DOWNSTREAM_PROCESSING,
              attachmentMessage.getAttachmentNames());
        });
    // @formatter:on
  }
}
