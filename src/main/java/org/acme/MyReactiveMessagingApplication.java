package org.acme;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.annotations.Blocking;
import org.eclipse.microprofile.reactive.messaging.*;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import java.time.Instant;
import java.util.stream.Stream;

@ApplicationScoped
public class MyReactiveMessagingApplication {

    @Inject
    @Channel("words-out")
    Emitter<String> emitter;

    /**
     * Sends message to the "words-out" channel, can be used from a JAX-RS resource or any bean of your application.
     * Messages are sent to the broker.
     **/
    void onStart(@Observes StartupEvent ev) {
        Stream.of("Hello", "with", "SmallRye", "reactive", "message").forEach(string -> emitter.send(string));
    }

    /**
     * Consume the message from the "words-in" channel, uppercase it and send it to the uppercase channel.
     * Messages come from the broker.
     **/
    @Incoming("words-in")
    //@Outgoing("uppercase")
    @Blocking(ordered = false)
    public Uni<Void> toUpperCase(Message<String> message) {
        System.out.println(Instant.now() + "Starting process [thread: " + getThreadInfo() + "]");
        return Uni.createFrom()
                .item(message)
                .onItem()
                .transformToUni(item -> Uni.createFrom().completionStage(message.ack()));
    }

    private String getThreadInfo() {
        return "ThreadInfo: "
                .concat(String.valueOf(Thread.currentThread().getId()))
                .concat("-")
                .concat(Thread.currentThread().getName());
    }

    /**
     * Consume the uppercase channel (in-memory) and print the messages.
     **/
    @Incoming("uppercase")
    public void sink(String word) {
        System.out.println(">> " + word);
    }
}
