package it.pagopa.pn.splitcon020.springbootcfg;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import it.pagopa.pn.commons.configs.aws.AwsConfigs;
import it.pagopa.pn.commons.exceptions.PnInternalException;
import it.pagopa.pn.commons.utils.MDCUtils;
import it.pagopa.pn.splitcon020.svc.Con020Service;
import it.pagopa.pn.splitcon020.PnSplitCon020Configs;
import it.pagopa.pn.splitcon020.svc.MessageConverterService;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.cloud.function.context.MessageRoutingCallback;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import java.util.UUID;
import java.util.function.Consumer;

@Configuration
@Slf4j
public class SQSConfig {

    public static final String INPUT_EVENTS_HANDLER_BEAN_NAME = "inputEventsHandler";
    public static final String NEW_ARCHIVE_HANDLER_BEAN_NAME = "newArchiveHandler";

    private final AwsConfigs awsConfigs;
    private final Con020Service con020Svc;
    private final MessageConverterService msg2dtoSvc;

    private final PnSplitCon020Configs appConfigs;

    public SQSConfig(AwsConfigs awsConfigs, Con020Service con020Svc, MessageConverterService msg2dtoSvc, PnSplitCon020Configs appConfigs) {
        this.awsConfigs = awsConfigs;
        this.con020Svc = con020Svc;
        this.msg2dtoSvc = msg2dtoSvc;
        this.appConfigs = appConfigs;
    }

    @Bean
    public AmazonSQSAsync amazonSQS() {

        String profileName = awsConfigs.getProfileName();

        AWSCredentialsProvider credentialProvider;
        //if( Strings.isBlank(profileName) ) {
            credentialProvider = DefaultAWSCredentialsProviderChain.getInstance();
        //}
        //else {
        //    credentialProvider = new ProfileCredentialsProvider( profileName );
        //}


        return AmazonSQSAsyncClientBuilder.standard()
                .withCredentials( credentialProvider )
                .withRegion( awsConfigs.getRegionCode() )
                .build();
    }

    @Bean
    public MessageRoutingCallback customRouter() {
        return new MessageRoutingCallback() {
            @Override
            public FunctionRoutingResult routingResult(Message<?> message) {
                setTraceId(message);
                return new FunctionRoutingResult(chooseHandler(message));
            }
        };
    }

    @Bean( name = INPUT_EVENTS_HANDLER_BEAN_NAME )
    public Consumer<Message<String>> inputEvents() {
        return (msg) -> this.con020Svc.receiveInputEvent(
                      this.msg2dtoSvc.parseInputEventFromJson( msg.getPayload() ) );
    }
    @Bean( name = NEW_ARCHIVE_HANDLER_BEAN_NAME )
    public Consumer<Message<String>> newArchiveEvents() {
        return (msg) -> this.con020Svc.startArchiveProcessing(
                  this.msg2dtoSvc.parseNewArchiveEventFromJson( msg.getPayload() )  );
    }




    private String chooseHandler(Message<?> message) {

        String queueName = (String) message.getHeaders().get("aws_receivedQueue");
        log.debug("received message from queue: {}", queueName);

        String handlerName;
        if( queueName.equals( appConfigs.getEventInputQueueName() )) {
            handlerName = INPUT_EVENTS_HANDLER_BEAN_NAME;
        }
        else if( queueName.equals( appConfigs.getArchiveToUnpackageQueueName() )) {
            handlerName = NEW_ARCHIVE_HANDLER_BEAN_NAME;
        }
        else {
            log.error("Undefined handler for queue: {}", queueName);
            throw new PnInternalException("Undefined handler for queue: " +queueName);
        }

        return handlerName;
    }

    private void setTraceId(Message<?> message) {
        MessageHeaders messageHeaders = message.getHeaders();
        MDCUtils.clearMDCKeys();

        if (messageHeaders.containsKey("aws_messageId")){
            String awsMessageId = messageHeaders.get("aws_messageId", String.class);
            MDC.put(MDCUtils.MDC_PN_CTX_MESSAGE_ID, awsMessageId);
        }

        if (messageHeaders.containsKey("X-Amzn-Trace-Id")){
            String traceId = messageHeaders.get("X-Amzn-Trace-Id", String.class);
            MDC.put(MDCUtils.MDC_TRACE_ID_KEY, traceId);
        } else {
            MDC.put(MDCUtils.MDC_TRACE_ID_KEY, String.valueOf(UUID.randomUUID()));
        }
    }

}