package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;

@Slf4j
@Service
@RequiredArgsConstructor
public class HubRouterClientGrpc {

    @GrpcClient("hub-router")
    private HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouter;

    public void sendRequest(DeviceActionRequest request) {
        try {
            hubRouter.handleDeviceAction(request);
        } catch (Exception e) {
            log.error("Ошибка при отправке запроса", e);
        }
    }
}
