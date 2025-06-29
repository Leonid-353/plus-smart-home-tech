package ru.yandex.practicum.telemetry.analyzer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.telemetry.analyzer.model.Action;
import ru.yandex.practicum.telemetry.analyzer.model.Scenario;

import java.util.List;

@Repository
public interface ActionRepository extends JpaRepository<Action, Long> {
    void deleteByScenario(Scenario scenario);

    List<Action> findAllByScenario(Scenario scenario);
}
