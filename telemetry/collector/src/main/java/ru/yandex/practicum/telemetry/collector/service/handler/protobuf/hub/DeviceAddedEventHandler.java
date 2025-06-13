package ru.yandex.practicum.telemetry.collector.service.handler.protobuf.hub;

import ru.yandex.practicum.grpc.telemetry.event.DeviceAddedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducerProto;

public class DeviceAddedEventHandler extends BaseHubEventHandler<DeviceAddedEventProto> {
    public DeviceAddedEventHandler(KafkaEventProducerProto producer) {
        super(producer);
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_ADDED;
    }

    @Override
    protected DeviceAddedEventProto mapToProto(HubEventProto event) {
        DeviceAddedEventProto deviceAddedEvent = event.getDeviceAdded();

        return DeviceAddedEventProto.newBuilder()
                .setId(deviceAddedEvent.getId())
                .setType(DeviceTypeProto.valueOf(deviceAddedEvent.getType().name()))
                .build();
    }

    @Override
    protected void setPayload(HubEventProto.Builder builder, DeviceAddedEventProto payload) {
        builder.setDeviceAdded(payload);
    }
}
