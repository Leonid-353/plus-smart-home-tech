package ru.yandex.practicum.telemetry.collector.service.handler.protobuf.sensor;

import ru.yandex.practicum.grpc.telemetry.event.ClimateSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducerProto;

public class ClimateSensorEventHandler extends BaseSensorEventHandler<ClimateSensorProto> {
    public ClimateSensorEventHandler(KafkaEventProducerProto producer) {
        super(producer);
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.CLIMATE_SENSOR;
    }

    @Override
    protected ClimateSensorProto mapToProto(SensorEventProto event) {
        ClimateSensorProto climateEvent = event.getClimateSensor();

        return ClimateSensorProto.newBuilder()
                .setTemperatureC(climateEvent.getTemperatureC())
                .setHumidity(climateEvent.getHumidity())
                .setCo2Level(climateEvent.getCo2Level())
                .build();
    }

    @Override
    protected void setPayload(SensorEventProto.Builder builder, ClimateSensorProto payload) {
        builder.setClimateSensor(payload);
    }
}
