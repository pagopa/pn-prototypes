package it.pagopa.pn.splitcon020.dao;

import it.pagopa.pn.splitcon020.PnSplitCon020Configs;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Component
@Slf4j
public class Con020ArchiveEntityDAO {

    private final DynamoDbClient dynamo;

    private final PnSplitCon020Configs appCfgs;

    public Con020ArchiveEntityDAO(DynamoDbClient dynamo, PnSplitCon020Configs appCfgs) {
        this.dynamo = dynamo;
        this.appCfgs = appCfgs;
    }

    private Map<String, AttributeValue> buildDynamoKey( String archiveFileKey ) {
        String hashKey = "CON20AR~" + archiveFileKey;

        Map<String, AttributeValue> key = new HashMap<>();
        key.put("hashKey", AttributeValue.builder().s(hashKey).build());
        key.put("sortKey", AttributeValue.builder().s("-").build());
        return key;
    }

    public void saveArchiveIfNotExsists(String archiveFileKey ) {
        UpdateItemRequestBuilderWrapper builder = UpdateItemRequestBuilderWrapper.instance()
                .tableName( appCfgs.getPaperEventEnrichmentTable() )
                .key( buildDynamoKey( archiveFileKey ) )
                .baseUpdateRequestAttribute( Instant.now(), "CON020Archive" )
                .updateField("archiveFileKey",  AttributeValue.fromS( archiveFileKey ))
                .updateIfFieldNotExists("archiveStatus", AttributeValue.fromS( "NEW" ))
            ;

        try {
            this.dynamo.updateItem( builder.build() );
        }
        catch (ConditionalCheckFailedException exc) {
            log.info("New event for existing archive entity archiveFileKey=" + archiveFileKey );
        }
    }


    public void updateStatus(String archiveFileKey, String newStatus) {
        String taskId = System.getProperty("TASK_ID");

        UpdateItemRequestBuilderWrapper builder = UpdateItemRequestBuilderWrapper.instance()
                .tableName( appCfgs.getPaperEventEnrichmentTable() )
                .key( buildDynamoKey( archiveFileKey ) )
                .baseUpdateRequestAttribute( Instant.now(), "CON020Archive" )
                .updateIfFieldExists("archiveStatus",  AttributeValue.fromS( newStatus ))
                .updateField("processingTask",  AttributeValue.fromS( taskId ));

        this.dynamo.updateItem( builder.build() );
    }
}
