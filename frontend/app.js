"use strict";
const el = (id) => document.getElementById(id);
const escapeHtml = (value) =>
  String(value ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
const labels = {
  queued: "В очереди",
  running: "Выполняется",
  review: "Ожидает решения",
  done: "Завершён",
  rejected: "Отклонён",
  failed: "Ошибка",
  cancelled: "Отменён",
};
const stamp = (n) =>
  n
    ? new Date(n * 1000).toLocaleTimeString("ru-RU", {
        fractionalSecondDigits: 3,
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      })
    : "—";
const tabs = {
  home: "Панель",
  queue: "Очередь",
  resources: "Ресурсы",
  timeline: "Хронология",
  log: "Журнал",
  review: "Согласования",
  scenarios: "Демонстрации",
};
let state,
  tab = "home",
  selected = 0,
  busy = false,
  noticeTimer,
  revision = 0;
const reviewRole = () => ["reviewer", "director"].includes(state.role);
function message(text) {
  el("toast").textContent = text;
  clearTimeout(noticeTimer);
  noticeTimer = setTimeout(() => (el("toast").textContent = ""), 5000);
}
async function api(route, data) {
  const r = await fetch(
    "/api/" + route,
    data === undefined
      ? { cache: "no-store" }
      : {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(data),
        },
  );
  const v = await r.json();
  if (!r.ok) throw Error(v.detail || "Команда не выполнена");
  return v;
}
function table(cols, rows) {
  return `<div class="scroll"><table><thead><tr>${cols.map((c) => `<th>${c}</th>`).join("")}</tr></thead><tbody>${rows.map((r) => `<tr>${r.map((c) => `<td>${c}</td>`).join("")}</tr>`).join("") || `<tr><td class="empty" colspan="${cols.length}">Нет записей</td></tr>`}</tbody></table></div>`;
}
const jobLink = (j) =>
  `<button class="link id" data-job="${j.id}">RUN-${j.id}</button>`;
function waitingReason(job, reason) {
  const text = escapeHtml(reason);
  if (job.status !== "queued" || !state.jobs.some(j => j.id === job.wait_for)) return text;
  return `<button class="link wait-link" data-job="${job.wait_for}" title="Открыть RUN-${job.wait_for}">${text}</button>`;
}
const badge = (j) =>
  `<span class="tag ${j.status === "review" ? "waiting_external" : j.status}">${labels[j.status]}</span>`;
let batchId=0;
function syncOptions(select,options){
  const html=options.map(([value,title])=>`<option value="${value}">${escapeHtml(title)}</option>`).join("");
  if(select.dataset.options!==html){
    const previous=select.value;
    select.innerHTML=html;
    select.dataset.options=html;
    if(options.some(([key])=>String(key)===previous))select.value=previous;
  }
}
function showScenario(batch){
  if(!batch)return '<p class="empty">Выберите сценарий и нажмите «Запустить демо».</p>';
  const ids=new Set(batch.jobs), jobs=state.jobs.filter(j=>ids.has(j.id));
  const events=state.events.filter(e=>ids.has(e.job));
  const origin=Math.min(...jobs.map(j=>j.created));
  const finish=j=>j.calculated_at||j.end;
  const active=jobs.some(j=>!finish(j));
  const end=Math.max(origin+.001,...jobs.map(j=>finish(j)||Date.now()/1000));
  const duration=end-origin;
  const pct=t=>Math.max(0,Math.min(100,100*(t-origin)/duration));
  const reasons=new Map();
  for(const e of events){
    if(/ожидает №|Ожидание предшественника|Все исполнители заняты/.test(e.message))reasons.set(e.job,e.message);
  }
  const done=jobs.filter(j=>["done","rejected","failed","cancelled"].includes(j.status)).length;
  const rows=jobs.map(j=>{
    const finishAt=finish(j)||end;
    const execution=j.start?`<i class="execution" style="left:${pct(j.start)}%;width:${Math.max(0,pct(finishAt)-pct(j.start))}%"></i>`:"";
    const bar=`<div class="interval" aria-hidden="true"><i class="waiting" style="left:${pct(j.created)}%;width:${Math.max(0,pct(j.start||finishAt)-pct(j.created))}%"></i>${execution}</div>`;
    return [jobLink(j),state.catalog[j.kind],j.day,badge(j),j.parent?"RUN-"+j.parent:"—",bar,waitingReason(j,j.status==="queued"?j.reason:reasons.get(j.id)||"—"),j.result||"—"];
  });
  return `<div class="pad"><h2>${state.scenarios[batch.scenario][0]}</h2><p class="lead">${state.scenarios[batch.scenario][1]}</p>
    <p class="batchstats">Заявки <b>${batch.requests}</b> · Запуски <b>${jobs.length}</b> · В очереди <b>${jobs.filter(j=>j.status==="queued").length}</b> · Выполняются <b>${jobs.filter(j=>j.status==="running").length}</b> · Завершены <b>${done}/${jobs.length}</b></p>
    <div class="legend"><span>Ожидание</span><span>Расчёт</span><small>Общая шкала от 0 до ${duration.toFixed(1)} с${active?" · в реальном времени":""}</small></div></div>`+
    table(["Запуск","Процесс","Дата","Статус","Зависит от","Ход выполнения","Причина последнего ожидания","Версия"],rows)+
    `<div class="pad"><h3>Журнал партии</h3></div>`+
    table(["№","От старта","Запуск","Операция"],events.slice().reverse().map(e=>[e.seq,Math.max(0,e.at-origin).toFixed(2)+" с",jobLink({id:e.job}),escapeHtml(e.message)]));
}
function render() {
  const jobs = state.jobs,
    review = reviewRole(),
    nav = review ? ["review"] : Object.keys(tabs).filter((t) => t !== "review" && (t !== "scenarios" || state.role === "admin"));
  el("scenario").hidden = state.role !== "admin";
  el("openDemos").hidden = state.role !== "admin";
  syncOptions(el("scenario"), Object.entries(state.scenarios).map(([k,v])=>[k,v[0]]));
  el("history").hidden = tab !== "scenarios" || !state.batches.length;
  syncOptions(el("batch"),state.batches.slice().reverse().map(b=>[b.id,`№${b.id} · ${state.scenarios[b.scenario][0]} · ${b.day}`]));
  const batch=state.batches.find(b=>b.id===batchId)||state.batches.at(-1);
  if(batch)el("batch").value=batch.id;
  el("role").value = state.role;
  el("demo").hidden = state.role !== "admin";
  el("launch").hidden = review;
  el("nav").innerHTML = nav
    .map(
      (t) =>
        `<button data-tab="${t}" ${tab === t ? 'aria-current="page"' : ""}>${tabs[t]}</button>`,
    )
    .join("");
  el("heading").textContent =
    tab === "home"
      ? state.role === "admin"
        ? "Системная панель"
        : "Панель аналитика"
      : tabs[tab];
  el("summary").innerHTML =
    tab !== "home"
      ? ""
      : [
          ["В очереди", "queued"],
          ["Выполняются", "running"],
          ["Ожидают решения", "review"],
        ]
          .map(
            ([name, k]) =>
              `<div class="metric"><div class="metricname">${name}</div><strong>${jobs.filter((j) => j.status === k).length}</strong></div>`,
          )
          .join("");
  let html;
  if (tab === "scenarios") html=showScenario(batch);
  else if (tab === "log")
    html = table(
      ["№", "Время", "Запуск", "Операция"],
      state.events
        .slice()
        .reverse()
        .map((e) => [
          e.seq,
          stamp(e.at),
          e.job ? "RUN-" + e.job : "—",
          escapeHtml(e.message),
        ]),
    );
  else if (tab === "resources")
    html = table(
      ["Запуск", "Ресурс", "Область", "Режим"],
      jobs
        .filter((j) => j.status === "running")
        .flatMap((j) =>
          j.resources.map(([k, d, m]) => [
            jobLink(j),
            state.labels[k],
            d,
            m === "W" ? "Запись" : "Чтение",
          ]),
        ),
    );
  else if (tab === "timeline")
    html = table(
      [
        "Порядок допуска",
        "Процесс",
        "Начало расчёта",
        "Конец расчёта",
        "Версия",
      ],
      jobs
        .filter((j) => j.start)
        .sort((a, b) => a.start - b.start || a.id - b.id)
        .map((j) => [
          jobLink(j),
          state.catalog[j.kind],
          stamp(j.start),
          stamp(j.calculated_at || j.end),
          j.result || "—",
        ]),
    );
  else
    html = table(
      [
        "Запуск",
        "Процесс",
        "Бизнес-дата",
        "Инициаторы",
        "Статус",
        "Причина ожидания / этап",
        "Действия",
      ],
      jobs
        .filter((j) => tab !== "queue" || j.status === "queued")
        .slice()
        .reverse()
        .map((j) => [
          jobLink(j),
          state.catalog[j.kind],
          j.day,
          j.initiators.map(escapeHtml).join(", "),
          badge(j),
          waitingReason(j,
            j.reason ||
              {
                pending: "Согласование риск-менеджера",
                reviewed: "Утверждение директора",
              }[j.version.approval] ||
              "—",
          ),
          `<button data-job="${j.id}">${review ? "Принять решение" : "Детали"}</button>`,
        ]),
    );
  el("view").innerHTML = html;
  if (el("details").open) renderDetail();
}
const number = new Intl.NumberFormat("ru-RU", {maximumFractionDigits: 2});
function approvalView(j) {
  const a = j.assessment;
  if (!a) return "<p>Чтобы загрузить сводку согласования, перезапустите сервер этапа 4.</p>";
  const director = state.role === "director";
  const indicator = {P1:"Кредитный риск",P2:"Рыночный риск",Z1:"Значения сделок",Z2:"Значения курсов",FX:"Значения справочника"}[j.kind];
  const measure = label => `<div class="approval-fact"><small>${label}</small>`;
  const delta = a.previous_total === null ? null : a.total - a.previous_total;
  let html = `<p class="approval-context">${director?"Утверждение к публикации":"Проверка расчёта"} · Бизнес-дата <b>${escapeHtml(j.day)}</b> · Инициатор ${j.initiators.map(escapeHtml).join(", ")}</p>
    <div class="approval-grid">
      ${measure(indicator+" · сумма по строкам")}<b>${number.format(a.total)} усл. ед.</b></div>
      ${measure("Версия результата")}<b>${escapeHtml(j.result)}</b></div>
      ${measure("Обработано строк")}<b>${a.count}</b></div>
      ${measure("Изменение к предыдущей публикации")}<b>${delta===null?"Первая публикация":(delta>0?"+":"")+number.format(delta)+" усл. ед."}</b>${delta===null?"":`<small>К версии ${escapeHtml(a.previous_id)}</small>`}</div>
    </div>
    <p class="approval-check">${escapeHtml(a.block_reason || "Входные версии актуальны. Построчная сверка пройдена.")}</p>`;
  if (director) {
    return html+`<h3 class="sectionlabel">Заключение риск-менеджера</h3>
      <p>${a.reviewed_by?`Версия ${escapeHtml(j.result)} согласована.`:"Согласование отсутствует."}</p>
      <p class="muted">${a.comment?`Комментарий: ${escapeHtml(a.comment)}`:"Комментарий к согласованию не добавлен."}</p>
      <p class="approval-note">После утверждения эта версия станет опубликованным результатом за ${escapeHtml(j.day)}.</p>`;
  }
  const sources = Object.entries(a.source_rows);
  const formulas = {P1:"Сделки × Курсы + Справочник",P2:"|Сделки − Курсы| × Справочник"};
  const formula = formulas[j.kind] || "Сумма входных значений + номер строки";
  html+=`<h3 class="sectionlabel">Проверка синтетических данных</h3>
    <p>Расчёт строки: (${formula}) × ${number.format(j.factor)}.</p>
    ${table(["Строка",...sources.map(([k])=>state.labels[k]),"Результат, усл. ед."],
      j.version.rows.map((v,i)=>[i+1,...sources.map(([,rows])=>number.format(rows[i])),number.format(v)]))}
    <p class="approval-note">Входные версии: ${Object.entries(j.inputs).map(([k,v])=>`${state.labels[k]} — ${escapeHtml(v)}`).join("; ")}.</p>`;
  return html;
}
function renderDetail() {
  const j = state.jobs.find((x) => x.id === selected);
  if (!j) {
    el("details").close();
    return;
  }
  el("jobTitle").textContent = `RUN-${j.id} · ${state.catalog[j.kind]}`;
  el("decision").hidden = !reviewRole() || j.status !== "review";
  const related = (id) => state.jobs.some(job => job.id === id) ? jobLink({id}) : `RUN-${id}`;
  const links = (j.rerun_from ? `<p>Перерасчёт для ${related(j.rerun_from)}</p>` : "") +
    (j.rerun_to ? `<p>Возвращён на перерасчёт → ${related(j.rerun_to)}</p>` : "");
  if (reviewRole()) {
    el("jobBody").innerHTML = links + approvalView(j);
    const approve = el("decision").querySelector('[data-decision="approve"]');
    approve.textContent = state.role === "director" ? "Утвердить к публикации" : "Согласовать версию";
    approve.disabled = !j.assessment || Boolean(j.assessment.block_reason);
    return;
  }
  const events = state.events.filter((e) => e.job === j.id);
  el("jobBody").innerHTML = links +
    `${badge(j)} <small>${j.day} · ${j.initiators.map(escapeHtml).join(", ")}</small>
 <h3 class="sectionlabel">Версия и результат</h3><p>${j.result ? escapeHtml(j.result) + " · " + j.version.rows.join(" · ") : "Расчёт ещё не завершён"}</p>
 <p class="muted">${Object.values(j.inputs).map(escapeHtml).join(" · ")}</p>
 <h3 class="sectionlabel">Области доступа</h3>${table(
   ["Ресурс", "Область", "Режим"],
   j.resources.map(([k, d, m]) => [
     state.labels[k],
     d,
     m === "W" ? "Запись" : "Чтение",
   ]),
 )}
 <h3 class="sectionlabel">Порядок операций</h3>${table(
   ["№", "Время", "Событие"],
   events.map((e) => [e.seq, stamp(e.at), escapeHtml(e.message)]),
 )}
 ${state.role === "admin" && ["queued", "running"].includes(j.status) ? '<button class="danger sp" id="cancel">Отменить запуск</button>' : ""}`;
}
async function refresh() {
  const generation = ++revision;
  const data = await api("state");
  if (generation !== revision) return;
  state = data;
  if (reviewRole()) tab = "review";
  else if (tab === "review") tab = "home";
  el("connection").dataset.status = data.worker_alive ? "online" : "degraded";
  el("connection").textContent = data.worker_alive
    ? "Система доступна" : "Сервер доступен, обработка очереди остановлена";
  render();
}
async function act(fn) {
  if (busy) return;
  busy = true;
  el("role").disabled = true;
  revision++;
  try {
    await fn();
    await refresh();
  } catch (e) {
    message(e.message);
  } finally {
    busy = false;
    el("role").disabled = false;
  }
}
el("role").onchange = () =>
  act(async () => {
    await api("role/" + el("role").value, {});
    tab = "home";
  });
el("demo").onclick = () =>
  act(async () => {
    const v = await api("demo?scenario="+encodeURIComponent(el("scenario").value), {});
    batchId = v.id;
    tab = "scenarios";
    message("Партия №"+v.id+" · "+v.day);
  });
el("openDemos").onclick = () => {
  if (state?.role !== "admin") return;
  tab = "scenarios";
  render();
  el("scenario").focus();
};
el("batch").onchange=()=>{batchId=Number(el("batch").value);render()};
el("launch").onclick = () => {
  const f = el("launchForm").elements;
  f.kind.innerHTML = Object.entries(state.catalog)
    .map(([k, v]) => `<option value="${k}">${v}</option>`)
    .join("");
  f.parent.innerHTML =
    '<option value="">Без зависимости</option>' +
    state.jobs
      .map((j) => `<option value="${j.id}">RUN-${j.id}</option>`)
      .join("");
  el("formDialog").showModal();
};
el("launchForm").onsubmit = (e) => {
  e.preventDefault();
  act(async () => {
    const data = Object.fromEntries(new FormData(e.target));
    data.key = crypto.randomUUID();
    let v = await api("run", data);
    el("formDialog").close();
    message("Создан RUN-" + v.id);
  });
};
document.addEventListener("click", (e) => {
  const b = e.target.closest("button");
  if (!b || !state) return;
  if (b.hasAttribute("data-close")) b.closest("dialog").close();
  if (b.dataset.tab) {
    tab = b.dataset.tab;
    render();
  }
  if (b.dataset.job) {
    selected = +b.dataset.job;
    el("comment").value = "";
    renderDetail();
    el("details").showModal();
  }
  if (b.id === "cancel")
    act(async () => {
      await api(`jobs/${selected}/cancel`, {});
      message("Запуск отменён");
    });
  if (b.dataset.decision)
    act(async () => {
      const j = state.jobs.find((x) => x.id === selected);
      const result = await api(`jobs/${selected}/decision`, {
        decision: b.dataset.decision,
        version: j.result,
        key: crypto.randomUUID(),
        comment: el("comment").value,
      });
      el("details").close();
      message(result.successor
        ? `RUN-${j.id} возвращён на перерасчёт. Создан RUN-${result.successor}.`
        : "Решение зарегистрировано");
    });
});
async function poll() {
  try {
    if (!busy) await refresh();
  } catch {
    el("connection").dataset.status = "offline";
    el("connection").textContent = "Нет связи с сервером";
  } finally {
    setTimeout(poll, 600);
  }
}
poll();
