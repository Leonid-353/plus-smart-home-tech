package ru.yandex.practicum.telemetry.collector.service.handler.avro.sensor;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SwitchSensorProto;
import ru.yandex.practicum.kafka.telemetry.event.SwitchSensorAvro;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducerAvro;

@Component
public class SwitchSensorEventHandlerAvro extends BaseSensorEventHandlerAvro<SwitchSensorAvro> {
    public SwitchSensorEventHandlerAvro(KafkaEventProducerAvro producer) {
        super(producer);
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.SWITCH_SENSOR;
    }

    @Override
    protected SwitchSensorAvro mapToAvro(SensorEventProto event) {
        SwitchSensorProto switchEvent = event.getSwitchSensor();

        return SwitchSensorAvro.newBuilder()
                .setState(switchEvent.getState())
                .build();
    }
}
