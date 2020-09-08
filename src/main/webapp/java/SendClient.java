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

public class SendClient {

        static final Gson GSON = new Gson();
        private static String Text;

        public static void task(String text) throws ServiceBusException, InterruptedException {
            // TODO Auto-generated method stub
            Text = text;

            String connectionString = "Endpoint=sb://internbus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=Wyg8Br7V97VvOJKv7YZ0TYPWONkm6PiNoDUWN1JF+tE=";
            TopicClient sendClient;
            //if(sendClient==null)
            sendClient = new TopicClient(new ConnectionStringBuilder(connectionString, "mytopic2"));

            sendMessagesAsync(sendClient).thenRunAsync(() -> sendClient.closeAsync());
        }

        static CompletableFuture<Void> sendMessagesAsync(TopicClient sendClient) {


            List<CompletableFuture> tasks = new ArrayList<>();

                final String messageId = (Text);
                Message message = new Message(Text.getBytes(UTF_8));
                message.setContentType("application/json");
                message.setLabel("Scientist");
                message.setMessageId(messageId);
                message.setTimeToLive(Duration.ofMinutes(2));
                System.out.printf("Message sending: Id = %s\n", message.getMessageId());
                tasks.add(
                        sendClient.sendAsync(message).thenRunAsync(() -> {
                            System.out.printf("\tMessage acknowledged: Id = %s\n", message.getMessageId());
                        }));

            return CompletableFuture.allOf(tasks.toArray(new CompletableFuture<?>[tasks.size()]));
        }

}
