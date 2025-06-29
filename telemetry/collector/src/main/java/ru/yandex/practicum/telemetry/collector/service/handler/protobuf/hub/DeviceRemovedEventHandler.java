package ru.yandex.practicum.telemetry.collector.service.handler.protobuf.hub;

import ru.yandex.practicum.grpc.telemetry.event.DeviceRemovedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducerProto;

public class DeviceRemovedEventHandler extends BaseHubEventHandler<DeviceRemovedEventProto> {
    public DeviceRemovedEventHandler(KafkaEventProducerProto producer) {
        super(producer);
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_REMOVED;
    }

    @Override
    protected DeviceRemovedEventProto mapToProto(HubEventProto event) {
        DeviceRemovedEventProto deviceRemovedEvent = event.getDeviceRemoved();

        return DeviceRemovedEventProto.newBuilder()
                .setId(deviceRemovedEvent.getId())
                .build();
    }

    @Override
    protected void setPayload(HubEventProto.Builder builder, DeviceRemovedEventProto payload) {
        builder.setDeviceRemoved(payload);
    }
}
