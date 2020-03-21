package ssedemo;

import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

@RestController
public class SimpleSSEApi {

    @GetMapping("/stocks/{code}")
    public Flux<ServerSentEvent<Stock>> stocks(@PathVariable("code") String code) {
        return Flux.interval(Duration.ofSeconds(1))
                .map(t -> Stock.builder()
                        .code(code)
                        .value(randomValue())
                        .build())
                .map(stock -> ServerSentEvent.builder(stock).build());
    }

    private int randomValue() {
        return ThreadLocalRandom.current().nextInt(1000) + 10000;
    }
}
