import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaConsumer {
    public static void main(String[] args) {
        new KafkaConsumer().start();
    }

    public void start(){

        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.setPrettyPrinting();
        Gson gson = gsonBuilder.create();
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"stream-consumer-id-1");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName());
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,1000);
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String,String> kStream =
                streamsBuilder.stream("bdccTopic", Consumed.with(Serdes.String(),Serdes.String()));

        final int[] counter = {0};
        List<Order> orders = new ArrayList<>();
       kStream.flatMapValues(value-> Arrays.asList(gson.fromJson(value,Order.class)))
                .map((k,v)-> new KeyValue<>(k,v))
               .filter((k,v)->v.ordre.equals("Vente"))
               .foreach((k,v)-> {System.out.println(k+" "+v);});
        // .foreach((k,v)-> {System.out.println(k+" "+v);});
        //kStream.foreach((k,v)-> {System.out.println(k+" "+v);});

        Topology topology = streamsBuilder.build();
        KafkaStreams kafkaStreams = new KafkaStreams(topology,properties);
        kafkaStreams.start();

    }
}

@NoArgsConstructor@AllArgsConstructor@Data@ToString
class Order{
    public String nom;
    public String ordre;
    public int nmbrAction;
    public double priceAction;
}

