package ru.yandex.practicum.telemetry.analyzer.service.handler.hub;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class HubEventHandlers {
    private final List<HubEventHandler> handlers;
    private Map<String, HubEventHandler> handlersMap;

    @PostConstruct
    public void init() {
        handlersMap = handlers.stream()
                .collect(Collectors.toMap(
                        HubEventHandler::getMessageType,
                        Function.identity()
                ));
    }

    public HubEventHandler getHandler(String messageType) {
        HubEventHandler handler = handlersMap.get(messageType);
        if (handler == null) {
            throw new IllegalArgumentException("Нет обработчика для типа: " + messageType);
        }
        return handler;
    }
}
