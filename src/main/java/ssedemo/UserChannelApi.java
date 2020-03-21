package ssedemo;

import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.objenesis.SpringObjenesis;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.UnicastProcessor;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@RestController
public class UserChannelApi {
    private UserChannels channels = new UserChannels();

    @GetMapping("/channels/users/{userId}/messages")
    public Flux<ServerSentEvent<String>> sse(@PathVariable("userId") Long userId) {
        return channels.connect(userId).toFlux().map(str -> ServerSentEvent.builder(str).build());
    }

    @PostMapping(path = "/channels/users/{userId}/messages", consumes = MediaType.TEXT_PLAIN_VALUE)
    public void post(@PathVariable("userId") Long userId, @RequestBody String message) {
        channels.post(userId, message);
    }

    public static class UserChannels {
        private ConcurrentHashMap<Long, UserChannel> map = new ConcurrentHashMap<>();

        public UserChannel connect(Long userId) {
            return map.computeIfAbsent(userId, key -> new UserChannel());
        }

        public void post(Long userId, String message) {
            Optional.ofNullable(map.get(userId)).ifPresentOrElse(ch -> ch.send(message), () -> {
            });
        }
    }

    public static class UserChannel {
        private Flux<String> flux;
        private FluxSink<String> sink;
        private final UnicastProcessor<String> processor;

        public UserChannel() {
            processor = UnicastProcessor.create();
            this.sink = processor.sink();
            this.flux = processor.share();
        }

        public void send(String message) {
            sink.next(message);
        }

        public Flux<String> toFlux() {
            return flux;
        }
    }
}
