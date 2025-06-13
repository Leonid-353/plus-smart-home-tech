package ru.yandex.practicum.telemetry.collector.service.handler.protobuf.sensor;

import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.TemperatureSensorProto;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducerProto;

public class TemperatureSensorEventHandler extends BaseSensorEventHandler<TemperatureSensorProto> {
    public TemperatureSensorEventHandler(KafkaEventProducerProto producer) {
        super(producer);
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.TEMPERATURE_SENSOR;
    }

    @Override
    protected TemperatureSensorProto mapToProto(SensorEventProto event) {
        TemperatureSensorProto temperatureEvent = event.getTemperatureSensor();

        return TemperatureSensorProto.newBuilder()
                .setTemperatureC(temperatureEvent.getTemperatureC())
                .setTemperatureF(temperatureEvent.getTemperatureF())
                .build();
    }

    @Override
    protected void setPayload(SensorEventProto.Builder builder, TemperatureSensorProto payload) {
        builder.setTemperatureSensor(payload);
    }
}
