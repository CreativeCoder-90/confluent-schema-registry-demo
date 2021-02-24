import com.model.User
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.*

class CustomConsumer {
    companion object{
        fun runConsumer(){
            var props = Properties()
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVER)
            props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConstants.CONSUMER_GROUP)
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000")
            props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,KafkaConstants.SCHEMA_REGISTRY_URL)
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaConstants.KAFKA_STRING_DESERIALIZER)
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer::class.java)
            props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,true)

            try {
                var consumer = KafkaConsumer<String,User>(props)
                consumer.subscribe(listOf(KafkaConstants.TOPIC))
                while (true){
                    var records: ConsumerRecords<String?, User?> = consumer.poll(100)
                    for (record in records){
                       val key = record.key()
                        val value = record.value()
                        System.out.println("key : $key,  value: $value ")
                        Thread.sleep(1000L)
                    }
                }
            }
            catch (e:Exception){
                e.printStackTrace()
            }
        }
    }
}