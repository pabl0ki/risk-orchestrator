"""Синтетические партии. Все обращения проходят штатный путь допуска."""
from datetime import date, timedelta
from .view import snapshot

SCENARIOS = {
    "basic": ("Чтение и запись", "Два расчёта читают сделки одной даты. Загрузка ждёт, а повторный запрос присоединяется к кредитному расчёту."),
    "pipeline": ("Закрытие трёх дат", "Для каждой даты последовательно загружаются сделки и курсы. После загрузки курсов кредитный и рыночный расчёты могут идти параллельно."),
    "capacity": ("Пиковая нагрузка", "Для шести дат одновременно поступают кредитные и рыночные расчёты. Доступны четыре слота. Повторные обращения не занимают дополнительных слотов."),
    "reference": ("Общий справочник", "Четыре расчёта читают справочник для двух дат. Обновление справочника ждёт завершения чтения. Ещё четыре расчёта зависят от завершения обновления."),
    "reapproval": ("Возврат на перерасчёт", "Кредитный расчёт Анны передаётся на согласование. Дождитесь завершения обновления справочника, верните расчёт от имени риск-менеджера, согласуйте новый RUN и утвердите его от имени директора."),
}


def launch_batch(engine, scenario):
    if scenario not in SCENARIOS:
        raise ValueError("Неизвестный демо-сценарий.")
    with engine.tx() as db:
        state = snapshot(db)
        used = [date.fromisoformat(j["day"]) for j in state["jobs"]]
        first = max([date(2026, 11, 21), *used]) + timedelta(days=1)
        ids = []

        def add(kind, offset, actor, *, parent=0, channel="manual", approval=False):
            jid = engine.add(db, dict(kind=kind, day=(first + timedelta(days=offset)).isoformat(),
                                         actor=actor, parent=parent, channel=channel, approval=approval))
            ids.append(jid)
            return jid

        if scenario == "basic":
            add("P1", 0, "Анна")
            add("P1", 0, "Регламент", channel="schedule")
            add("P2", 0, "Илья")
            add("Z1", 0, "Загрузка")
        elif scenario == "pipeline":
            for offset in range(3):
                deals = add("Z1", offset, "Загрузка сделок")
                rates = add("Z2", offset, "Загрузка курсов", parent=deals)
                add("P1", offset, "Анна", parent=rates)
                add("P2", offset, "Илья", parent=rates)
        elif scenario == "capacity":
            for offset in range(6):
                add("P1", offset, "Анна")
                add("P1", offset, "Регламент", channel="schedule")
                add("P2", offset, "Илья")
        elif scenario == "reapproval":
            add("P1", 0, "Анна", approval=True)
            add("FX", 0, "Владелец справочника")
        else:
            for offset in range(2):
                add("P1", offset, "Анна")
                add("P2", offset, "Илья")
            reference = add("FX", 0, "Владелец справочника")
            for offset in range(2):
                add("P1", offset, "Анна", parent=reference)
                add("P2", offset, "Илья", parent=reference)

        batches = state.setdefault("demo_batches", [])
        batch = dict(id=len(batches)+1, scenario=scenario, day=first.isoformat(),
                     jobs=list(dict.fromkeys(ids)), requests=len(ids))
        db.log_event("demo_batch","batch",batch["id"],batch)
        return dict(**batch, ids=ids)
