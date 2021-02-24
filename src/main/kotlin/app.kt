import java.util.*

fun main() {
    println("press 1 for producer & 2 for consumer:")
when(Scanner(System.`in`).nextInt()){
    1 -> CustomProducer.runProducer()
    2 -> CustomConsumer.runConsumer()
}
}