package ru.yandex.practicum.telemetry.collector.service.handler.protobuf.hub;

import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducerProto;

import java.util.List;

public class ScenarioAddedEventHandler extends BaseHubEventHandler<ScenarioAddedEventProto> {
    public ScenarioAddedEventHandler(KafkaEventProducerProto producer) {
        super(producer);
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_ADDED;
    }

    @Override
    protected ScenarioAddedEventProto mapToProto(HubEventProto event) {
        ScenarioAddedEventProto scenarioAddedEvent = event.getScenarioAdded();

        List<ScenarioConditionProto> conditions = scenarioAddedEvent.getConditionsList().stream()
                .map(this::convertCondition)
                .toList();

        List<DeviceActionProto> actions = scenarioAddedEvent.getActionsList().stream()
                .map(this::convertAction)
                .toList();

        return ScenarioAddedEventProto.newBuilder()
                .setName(scenarioAddedEvent.getName())
                .addAllConditions(conditions)
                .addAllActions(actions)
                .build();
    }

    @Override
    protected void setPayload(HubEventProto.Builder builder, ScenarioAddedEventProto payload) {
        builder.setScenarioAdded(payload);
    }

    private ScenarioConditionProto convertCondition(ScenarioConditionProto condition) {
        ScenarioConditionProto.Builder builder = ScenarioConditionProto.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(ConditionTypeProto.valueOf(condition.getType().name()))
                .setOperation(ConditionOperationProto.valueOf(condition.getOperation().name()));
        switch (condition.getValueCase()) {
            case BOOL_VALUE -> builder.setBoolValue(condition.getBoolValue());
            case INT_VALUE -> builder.setIntValue(condition.getIntValue());
            case VALUE_NOT_SET -> {
            }
        }

        return builder.build();
    }

    private DeviceActionProto convertAction(DeviceActionProto action) {
        DeviceActionProto.Builder builder = DeviceActionProto.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(ActionTypeProto.valueOf(action.getType().name()));

        if (action.hasValue()) {
            builder.setValue(action.getValue());
        }

        return builder.build();
    }
}
