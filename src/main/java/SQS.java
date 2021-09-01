import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import com.amazonaws.services.lambda.model.*;
import java.util.List;

public class SQS {
    static AmazonSQS sqs= AmazonSQSClientBuilder.standard().
            withEndpointConfiguration(new AwsClientBuilder.
                    EndpointConfiguration("http://localhost:4566", "us-west-2"))
            .build();
    static String queueUrl="http://localhost:4566/000000000000/parkingQ";

    public static String ReadMessage(){
        System.out.println("Reading Messages From Queue");
        ReceiveMessageRequest req=new ReceiveMessageRequest().withQueueUrl(queueUrl).withVisibilityTimeout(0).withWaitTimeSeconds(10).withMaxNumberOfMessages(10);
        List<Message> messages=sqs.receiveMessage(req).getMessages();


        String Json="";
        for(Message m:messages){
            System.out.println(m.toString());
            Json=m.getBody();
            DeleteMessageResult res= sqs.deleteMessage(queueUrl,m.getReceiptHandle());
            break;

        }

        System.out.println("Queue Scanned Successfully!");

        return Json;

    }
    public static int getCount(){
        ReceiveMessageRequest req=new ReceiveMessageRequest().withQueueUrl(queueUrl).withVisibilityTimeout(0).withWaitTimeSeconds(10).withMaxNumberOfMessages(10);
        List<Message> messages=sqs.receiveMessage(req).getMessages();
        return messages.size();
    }

}
