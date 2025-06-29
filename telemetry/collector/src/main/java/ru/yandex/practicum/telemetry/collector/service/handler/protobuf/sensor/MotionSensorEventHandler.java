package ru.yandex.practicum.telemetry.collector.service.handler.protobuf.sensor;

import ru.yandex.practicum.grpc.telemetry.event.MotionSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducerProto;

public class MotionSensorEventHandler extends BaseSensorEventHandler<MotionSensorProto> {
    public MotionSensorEventHandler(KafkaEventProducerProto producer) {
        super(producer);
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.MOTION_SENSOR;
    }

    @Override
    protected MotionSensorProto mapToProto(SensorEventProto event) {
        MotionSensorProto motionEvent = event.getMotionSensor();

        return MotionSensorProto.newBuilder()
                .setLinkQuality(motionEvent.getLinkQuality())
                .setMotion(motionEvent.getMotion())
                .setVoltage(motionEvent.getVoltage())
                .build();
    }

    @Override
    protected void setPayload(SensorEventProto.Builder builder, MotionSensorProto payload) {
        builder.setMotionSensor(payload);
    }
}
