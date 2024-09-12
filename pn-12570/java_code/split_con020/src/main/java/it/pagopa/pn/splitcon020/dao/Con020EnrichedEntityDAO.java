package it.pagopa.pn.splitcon020.dao;

import it.pagopa.pn.splitcon020.PnSplitCon020Configs;
import it.pagopa.pn.splitcon020.dto.Con020InputEventDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Component
@Slf4j
public class Con020EnrichedEntityDAO {

    private static final long MILLISECONDS_IN_YEAR = 365 * 24 * 3600 * 1000l;

    private final DynamoDbClient dynamo;

    private final PnSplitCon020Configs appCfgs;

    public Con020EnrichedEntityDAO(DynamoDbClient dynamo, PnSplitCon020Configs appCfgs) {
        this.dynamo = dynamo;
        this.appCfgs = appCfgs;
    }

    private Map<String, AttributeValue> buildDynamoKey(
            String archiveFileKey,
            String requestId,
            String registeredLetterCode
    ) {
        String hashKey = "CON20EN~" + archiveFileKey + "_" + requestId + "_" + registeredLetterCode;

        Map<String, AttributeValue> key = new HashMap<>();
        key.put("hashKey", AttributeValue.builder().s(hashKey).build());
        key.put("sortKey", AttributeValue.builder().s("-").build());
        return key;
    }

    private Map<String, AttributeValue> buildDynamoKey( Con020InputEventDto inEvt ) {
        String archiveFileKey = inEvt.getAttachmentFileKey();

        String requestId = inEvt.getAnalogMail().getRequestId();
        String registeredLetterCode = inEvt.getAnalogMail().getRegisteredLetterCode();
        return  buildDynamoKey( archiveFileKey, requestId, registeredLetterCode );
    }

    private AttributeValue buildMetadata(Con020InputEventDto inputEvent) {
        String requestId = inputEvent.getAnalogMail().getRequestId();
        String iun = requestId.replaceFirst(".*\\.IUN_([^\\.]*)\\..*", "$1");
        String recIndex = requestId.replaceFirst(".*\\.RECINDEX_([^\\.]*)\\..*", "$1");

        String generationTime = inputEvent.getAnalogMail().getStatusDateTime();
        String registeredLetterCode = inputEvent.getAnalogMail().getRegisteredLetterCode();
        String eventTime = inputEvent.getEventTimestamp();

        Map<String, AttributeValue> metadata = new HashMap<>();
        metadata.put("iun",  AttributeValue.fromS( iun ) );
        metadata.put("generationTime",  AttributeValue.fromS( generationTime ) );
        metadata.put("recIndex",  AttributeValue.fromN( recIndex ) );
        metadata.put("sendRequestId",  AttributeValue.fromS( requestId ) );
        metadata.put("registeredLetterCode",  AttributeValue.fromS( registeredLetterCode ) );
        metadata.put("eventTime",  AttributeValue.fromS( eventTime ) );
        metadata.put("archiveFileKey",  AttributeValue.fromS( inputEvent.getAttachmentFileKey() ) );

        return AttributeValue.fromM( metadata );
    }

    public void saveInputEventMetadata( Con020InputEventDto inputEvent ) {
        UpdateItemRequestBuilderWrapper builder = UpdateItemRequestBuilderWrapper.instance()
                .tableName( appCfgs.getPaperEventEnrichmentTable() )
                .key( buildDynamoKey( inputEvent ) )
                .baseUpdateRequestAttribute( Instant.now(), "CON020Enriched" )
                .updateIfFieldNotExists("metadata", buildMetadata( inputEvent ))
            ;

        try {
            this.dynamo.updateItem( builder.build() );
        }
        catch (ConditionalCheckFailedException exc) {
            log.warn("Duplicated metadata update for " + inputEvent + ": ", exc );
        }
    }


    public void writePdfUriIntoCon020EventEntity( String archiveFileKey, String requestId, String registeredLetterCode, String pdfFileKey ) {
        System.out.println("Update printedPdf" +
                " archiveFileKey=" + archiveFileKey +
                " requestId=" + requestId +
                " registeredLetterCode=" + registeredLetterCode +
                " pdfFileKey=" + pdfFileKey
            );
        UpdateItemRequestBuilderWrapper builder = UpdateItemRequestBuilderWrapper.instance()
                .tableName( appCfgs.getPaperEventEnrichmentTable() )
                .key( buildDynamoKey( archiveFileKey, requestId, registeredLetterCode) )
                .baseUpdateRequestAttribute( Instant.now(), "CON020Enriched" )
                .updateIfFieldNotExists("printedPdf", AttributeValue.fromS(pdfFileKey ))
                ;

        try {
            this.dynamo.updateItem( builder.build() );
        }
        catch (ConditionalCheckFailedException exc) {
            log.warn("Duplicated printedPdf update for " +
                    "archiveFileKey=" + archiveFileKey + " " +
                    "requestId=" + requestId + " " +
                    "registeredLetterCode=" + registeredLetterCode, exc );
        }
    }

}
