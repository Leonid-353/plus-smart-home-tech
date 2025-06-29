package ru.yandex.practicum.telemetry.collector.service.handler.avro.sensor;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.LightSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.LightSensorAvro;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducerAvro;

@Component
public class LightSensorEventHandlerAvro extends BaseSensorEventHandlerAvro<LightSensorAvro> {
    public LightSensorEventHandlerAvro(KafkaEventProducerAvro producer) {
        super(producer);
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.LIGHT_SENSOR;
    }

    @Override
    protected LightSensorAvro mapToAvro(SensorEventProto event) {
        LightSensorProto lightEvent = event.getLightSensor();

        return LightSensorAvro.newBuilder()
                .setLinkQuality(lightEvent.getLinkQuality())
                .setLuminosity(lightEvent.getLuminosity())
                .build();
    }
}
