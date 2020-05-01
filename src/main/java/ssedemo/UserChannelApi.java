package ssedemo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
public class UserChannelApi {
    private static Logger logger = LoggerFactory.getLogger(UserChannelApi.class);

    private UserChannels channels = new UserChannels();
    private AtomicInteger id = new AtomicInteger();

    @GetMapping("/channels/users/{userId}/messages")
    public Flux<ServerSentEvent<String>> connect(@PathVariable("userId") Long userId) {
        int no = id.getAndAdd(1);
        Flux<String> userStream = channels.connect(userId).toFlux();
        Flux<String> tickStream = Flux.interval(Duration.ofSeconds(5))
                .map(tick -> "HEARTBEAT " + no);
        return Flux.merge(userStream, tickStream)
                .map(str -> ServerSentEvent.builder(str).build());
    }

    @PostMapping(path = "/channels/users/{userId}/messages", consumes = MediaType.TEXT_PLAIN_VALUE)
    public void post(@PathVariable("userId") Long userId, @RequestBody String message) {
        channels.post(userId, message);
    }

    public static class UserChannels {
        private ConcurrentHashMap<Long, UserChannel> map = new ConcurrentHashMap<>();

        public UserChannel connect(Long userId) {
            return map.computeIfAbsent(userId, key -> new UserChannel().onClose(() ->
                    map.remove(userId)));
        }

        public void post(Long userId, String message) {
            Optional.ofNullable(map.get(userId)).ifPresentOrElse(ch -> ch.send(message), () -> {
            });
        }
    }

    public static class UserChannel {
        private EmitterProcessor<String> processor;
        private Flux<String> flux;
        private FluxSink<String> sink;
        private Runnable closeCallback;

        public UserChannel() {
            processor = EmitterProcessor.create();
            this.sink = processor.sink();
            this.flux = processor
                    .doOnCancel(() -> {
                        logger.info("doOnCancel, downstream " + processor.downstreamCount());
                        if (processor.downstreamCount() == 1) close();
                    })
                    .doOnTerminate(() -> {
                        logger.info("doOnTerminate, downstream " + processor.downstreamCount());
                    });
        }

        public void send(String message) {
            sink.next(message);
        }

        public Flux<String> toFlux() {
            return flux;
        }

        private void close() {
            if (closeCallback != null) closeCallback.run();
            sink.complete();
        }

        public UserChannel onClose(Runnable closeCallback) {
            this.closeCallback = closeCallback;
            return this;
        }
    }
}
