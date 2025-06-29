package ru.yandex.practicum.telemetry.collector.service.handler.protobuf.sensor;

import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SwitchSensorProto;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducerProto;

public class SwitchSensorEventHandler extends BaseSensorEventHandler<SwitchSensorProto> {
    public SwitchSensorEventHandler(KafkaEventProducerProto producer) {
        super(producer);
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.SWITCH_SENSOR;
    }

    @Override
    protected SwitchSensorProto mapToProto(SensorEventProto event) {
        SwitchSensorProto switchEvent = event.getSwitchSensor();

        return SwitchSensorProto.newBuilder()
                .setState(switchEvent.getState())
                .build();
    }

    @Override
    protected void setPayload(SensorEventProto.Builder builder, SwitchSensorProto payload) {
        builder.setSwitchSensor(payload);
    }
}
