package com.example.demo;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;

@RestController
public class SQSController {
    private static final String QUEUE_NAME = "MyQueue";

    private final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
    private String queueUrl;

    public SQSController() {
        // Ensure the queue exists
        try {
            this.queueUrl = sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();
        } catch (QueueDoesNotExistException e) {
            this.queueUrl = sqs.createQueue(QUEUE_NAME).getQueueUrl();
        }
    }


    @GetMapping("/send")
    public String sendMessage() {
        SendMessageRequest sendMsgRequest = new SendMessageRequest()
                .withQueueUrl(queueUrl)
                .withMessageBody("Hello, world!");
        sqs.sendMessage(sendMsgRequest);

        return "Message sent!";
    }


    @Async
    @Scheduled(fixedRate = 5000)
    public void receiveMessages() {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest()
                .withQueueUrl(queueUrl)
                .withMaxNumberOfMessages(1);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();

        for (Message m : messages) {
            System.out.println("Received message: " + m.getBody());
            sqs.deleteMessage(queueUrl, m.getReceiptHandle());
            System.out.println("Deleted message");
        }
    }
}
