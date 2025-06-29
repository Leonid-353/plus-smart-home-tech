package ru.yandex.practicum.telemetry.collector.service.handler.protobuf.sensor;

import ru.yandex.practicum.grpc.telemetry.event.LightSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducerProto;

public class LightSensorEventHandler extends BaseSensorEventHandler<LightSensorProto> {
    public LightSensorEventHandler(KafkaEventProducerProto producer) {
        super(producer);
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.LIGHT_SENSOR;
    }

    @Override
    protected LightSensorProto mapToProto(SensorEventProto event) {
        LightSensorProto lightEvent = event.getLightSensor();

        return LightSensorProto.newBuilder()
                .setLinkQuality(lightEvent.getLinkQuality())
                .setLuminosity(lightEvent.getLuminosity())
                .build();
    }

    @Override
    protected void setPayload(SensorEventProto.Builder builder, LightSensorProto payload) {
        builder.setLightSensor(payload);
    }
}
