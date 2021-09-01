import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.gson.Gson;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Dynamo {
    static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder
                    .EndpointConfiguration("http://localhost:4566", "us-west-2"))
            .build();

    static DynamoDB dynamodb = new DynamoDB(client);
    static String tableName = "ParkingData";

    public static void createTable() {
        try {
            System.out.println("Table Creating .....");
            Table table = dynamodb.createTable(tableName,
                    Arrays.asList(new KeySchemaElement("year", KeyType.HASH),
                            new KeySchemaElement("title", KeyType.RANGE)),
                    Arrays.asList(new AttributeDefinition("year", ScalarAttributeType.N),
                            new AttributeDefinition("title", ScalarAttributeType.S)),
                    new ProvisionedThroughput(10L, 10L));

            System.out.println("Table " + tableName + " Created!!!" + "with status " +
                    (table.getDescription() != null ? table.getDescription().getTableStatus() : "Inactive"));
            table.waitForActive();
            System.out.println("Table " + tableName + "Created!!!"
                    + "with Status " + table.getDescription().getTableStatus());

        } catch (Exception e) {
            System.out.println("Unable to create Table!!!!");
        }
    }

    public static void insertData(ParkInfo p) {
        int year = 2021;


        final Map<String, Object> infoMap = new HashMap<String, Object>();
        infoMap.put("Vehicle_Name", p.getV_Name());
        infoMap.put("email", p.getEmail());
        infoMap.put("Check_In", p.getParkin());
        infoMap.put("Check_out", null);

        try {
            Table table = dynamodb.getTable(tableName);
            PutItemOutcome outcome = table
                    .putItem(new Item().withPrimaryKey("year", year, "title", p.getV_Name())
                            .withMap("info", infoMap));

            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());

        } catch (Exception E) {
            System.out.println("Error Inserting Code");
        }

    }

    public static String updateData(String vname, String endtime) {
        int year = 2021;

        UpdateItemSpec updateItemSpec = new UpdateItemSpec()
                .withPrimaryKey(new PrimaryKey("year", year, "title", vname))
                .withUpdateExpression("set info.Check_out = :val")
                .withValueMap(new ValueMap().withString(":val", endtime))
                .withReturnValues(ReturnValue.UPDATED_NEW);


        try {
            Table table = dynamodb.getTable(tableName);
            System.out.println("Attempting a conditional update...");
            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
            System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());
            ParkInfo info = new Gson().fromJson(outcome.getItem().toJSONPretty(), ParkInfo.class);

            //Converted Object to JSON Format
            Gson g = new Gson();
            String str = g.toJson(info.getInfo());
            ParkInfo inf = new Gson().fromJson(str, ParkInfo.class);

            return inf.getEmail();
        } catch (Exception e) {
            System.err.println("Unable to update item: " + year + " " + vname);
            System.err.println(e.getMessage());
            return "";
        }
    }


    public static void scanTable(){
        System.out.println("Scanning the Table.....");
        ScanRequest scanRequest = new ScanRequest()
                .withTableName(tableName);

        ScanResult result = client.scan(scanRequest);
        for (Map<String, AttributeValue> item : result.getItems()){
            System.out.println(item);
        }
    }

    public static void main(String[] args) {
        scanTable();
    }
}


