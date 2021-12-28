import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueNameExistsException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import java.util.List;


public class EC2_Worker{

  public static void main(String[] args) {
	  
	  //initializing queue name
      String queueName1 = "Inbox";
      String queueName2 = "Outbox";
      
      //queue url
      String Url="https://sqs.us-east-1.amazonaws.com/004250956885/Inbox";

      //initializing sqs client & region
      SqsClient sqsClient = SqsClient.builder()
              .region(Region.US_EAST_1)
              .build();
      
      //String queueUrl1 = createQueue(sqsClient, queueName1 );
      //String queueUrl2 = createQueue(sqsClient, queueName2 );

      //to list all queues
      //listQueues(sqsClient);

      //deleteSQSQueue(sqsClient, queueName1); 
      //deleteSQSQueue(sqsClient, queueName2);
      RetrieveMessage(sqsClient, Url);
      sqsClient.close();


  }


  public static String createQueue(SqsClient sqsClient,String queueName ) {

      try {
          System.out.println("\n"+ queueName+ " que has been created.");

          //create queue request 
          CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
              .queueName(queueName)
              .build();

          sqsClient.createQueue(createQueueRequest);
          
          //System.out.println("\nGet queue url");

        //get queue url response
          GetQueueUrlResponse getQueueUrlResponse =
              sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
          String queueUrl = getQueueUrlResponse.queueUrl();
          return queueUrl;

      } catch (SqsException e) {
          System.err.println(e.awsErrorDetails().errorMessage());
          System.exit(1);
      }
      return "";

  }

  public static void listQueues(SqsClient sqsClient) {
	  
	  //list of queues
      System.out.println("\nList Queues: ");
      String prefix = "que";

      try {
          ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder().queueNamePrefix(prefix).build();
          ListQueuesResponse listQueuesResponse = sqsClient.listQueues(listQueuesRequest);

          for (String url : listQueuesResponse.queueUrls()) {
            //  System.out.println(url);
          }

      } catch (SqsException e) {
          System.err.println(e.awsErrorDetails().errorMessage());
          System.exit(1);
      }
  }

    public static void deleteSQSQueue(SqsClient sqsClient, String queueName) {

        try {

            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();

            String queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();
            
            System.out.println("\nQueue has been deleted");
            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build();

            sqsClient.deleteQueue(deleteQueueRequest);

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    
    public static String RetrieveMessage(SqsClient sqsClient, String Url) {
	        
        try {
            // Receive messages from the queue
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(Url)
                .build();

            List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();
            String message_content = "";

            // Print out the messages
            System.out.println("\nMassege content: ");
             for (Message m : messages) {
                System.out.println("\n" +m.body());
                 message_content = m.body();
             }

             
            deleteMessages(sqsClient, Url,  messages);
            return(message_content);

        } catch (QueueNameExistsException e) {
            throw e;
        }
        
    }
    
    public static void deleteMessages(SqsClient sqsClient, String queueUrl,  List<Message> messages) {

  
        try {
            for (Message message : messages) {
                DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
                sqsClient.deleteMessage(deleteMessageRequest);
                System.out.println("\nMessage is deleted.");
            }
  
  
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
   }
 
}

