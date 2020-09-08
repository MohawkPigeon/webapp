package main.java;

import com.google.gson.reflect.TypeToken;
import com.microsoft.azure.servicebus.*;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.google.gson.Gson;
import static java.nio.charset.StandardCharsets.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import org.apache.commons.cli.*;
import org.apache.commons.cli.DefaultParser;

public class MyServiceBusTopicClient {

    static final Gson GSON = new Gson();
    static int count = 0;

    public static void main(String[] args) throws Exception, ServiceBusException {
        String connectionString = "Endpoint=sb://internbus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=Wyg8Br7V97VvOJKv7YZ0TYPWONkm6PiNoDUWN1JF+tE=";
        SubscriptionClient subscription1Client = new SubscriptionClient(new ConnectionStringBuilder(connectionString, "mytopic/Subscriptions/S1"), ReceiveMode.PEEKLOCK);
        SubscriptionClient subscription2Client = new SubscriptionClient(new ConnectionStringBuilder(connectionString, "mytopic/Subscriptions/S2"), ReceiveMode.PEEKLOCK);
        SubscriptionClient subscription3Client = new SubscriptionClient(new ConnectionStringBuilder(connectionString, "mytopic/Subscriptions/S3"), ReceiveMode.PEEKLOCK);

        registerMessageHandlerOnClient(subscription1Client);
        registerMessageHandlerOnClient(subscription2Client);
        registerMessageHandlerOnClient(subscription3Client);
    }

    static void registerMessageHandlerOnClient(SubscriptionClient receiveClient) throws Exception {

        // register the RegisterMessageHandler callback
        IMessageHandler messageHandler = new IMessageHandler() {
            // callback invoked when the message handler loop has obtained a message
            public CompletableFuture<Void> onMessageAsync(IMessage message) {
                // receives message is passed to callback
                if (message != null ) {
                    count++;
                    byte[] body = message.getBody();
                    String text = new String(body, UTF_8);

                    if(count%3==0){

                        SendClient sendClient = new SendClient();
                    try {
                        sendClient.task(text);
                    } catch (ServiceBusException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    }
                    System.out.printf(
                            "\n\t\t\t\t%s Message received: \n\t\t\t\t\t\tMessageId = %s \n\t\t\t\t\t\t",
                            receiveClient.getEntityPath(),
                            message.getMessageId());
                    System.out.println(text);
                }
                return receiveClient.completeAsync(message.getLockToken());
            }

            public void notifyException(Throwable throwable, ExceptionPhase exceptionPhase) {
                System.out.printf(exceptionPhase + "-" + throwable.getMessage());
            }
        };


        receiveClient.registerMessageHandler(
                messageHandler,
                // callback invoked when the message handler has an exception to report
                // 1 concurrent call, messages aren't auto-completed, auto-renew duration
                new MessageHandlerOptions(1, false, Duration.ofMinutes(1)));

    }
}
