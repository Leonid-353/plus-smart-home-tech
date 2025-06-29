package ru.yandex.practicum.telemetry.collector.service.handler.protobuf.hub;

import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioRemovedEventProto;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducerProto;

public class ScenarioRemovedEventHandler extends BaseHubEventHandler<ScenarioRemovedEventProto> {
    public ScenarioRemovedEventHandler(KafkaEventProducerProto producer) {
        super(producer);
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_REMOVED;
    }

    @Override
    protected ScenarioRemovedEventProto mapToProto(HubEventProto event) {
        ScenarioRemovedEventProto scenarioRemovedEvent = event.getScenarioRemoved();

        return ScenarioRemovedEventProto.newBuilder()
                .setName(scenarioRemovedEvent.getName())
                .build();
    }

    @Override
    protected void setPayload(HubEventProto.Builder builder, ScenarioRemovedEventProto payload) {
        builder.setScenarioRemoved(payload);
    }
}
