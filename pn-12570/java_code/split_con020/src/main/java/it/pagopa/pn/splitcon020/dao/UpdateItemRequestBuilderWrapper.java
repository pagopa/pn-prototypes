package it.pagopa.pn.splitcon020.dao;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class UpdateItemRequestBuilderWrapper {

    private static final long MILLISECONDS_IN_YEAR = 365 * 24 * 3600 * 1000l;

    private final UpdateItemRequest.Builder builder = UpdateItemRequest.builder();
    private final Map<String, AttributeValue> attributeValues = new HashMap<>();
    private final Map<String, String> attributeNames = new HashMap<>();

    private String updateExpression;

    private UpdateItemRequestBuilderWrapper() {}

    public static UpdateItemRequestBuilderWrapper instance() {
        return new UpdateItemRequestBuilderWrapper();
    }

    public UpdateItemRequestBuilderWrapper tableName( String tableName ) {
        this.builder.tableName( tableName );
        return this;
    }

    public UpdateItemRequestBuilderWrapper key(Map<String, AttributeValue> dynamoKey ) {
        this.builder.key( dynamoKey );
        return this;
    }

    public UpdateItemRequestBuilderWrapper baseUpdateRequestAttribute( Instant operationTime, String entityName ) {

        this.updateExpression =
                "SET " +
                        " recordCreationTime = if_not_exists( recordCreationTime, :operationTime ), " +
                        " lastModificationTime = :operationTime, " +
                        " #ttlField = :ttlVal, " +
                        " entityName = :entityName ";


        String operationTimeString = operationTime.toString()
                .replaceFirst("\\.\\([0-9][0-9][0-9]\\)[0-9]*Z$", ".\\1Z");
        long ttl = operationTime.toEpochMilli() + MILLISECONDS_IN_YEAR;

        attributeValues.put( ":operationTime", AttributeValue.fromS( operationTimeString ) );
        attributeValues.put( ":ttlVal", AttributeValue.fromN( "" + ttl ));
        attributeValues.put(":entityName", AttributeValue.fromS( entityName ));


        attributeNames.put("#ttlField", "ttl");

        return this;
    }

    public UpdateItemRequestBuilderWrapper updateIfFieldNotExists(String fieldName, AttributeValue value ) {
        updateField( fieldName, value );

        builder.conditionExpression(" attribute_not_exists( " + fieldName +" )");
        return this;
    }

    public UpdateItemRequestBuilderWrapper updateIfFieldExists(String fieldName, AttributeValue value ) {
        updateField( fieldName, value );

        builder.conditionExpression(" attribute_exists( " + fieldName +" )");
        return this;
    }

    public UpdateItemRequestBuilderWrapper updateField(String fieldName, AttributeValue value) {
        updateExpression += ", " + fieldName + " = :" + fieldName;
        attributeValues.put( ":" + fieldName, value);

        return this;
    }

    public UpdateItemRequest build() {
        builder.updateExpression( updateExpression );
        builder.expressionAttributeNames( attributeNames );
        builder.expressionAttributeValues( attributeValues );

        return builder.build();
    }

}
