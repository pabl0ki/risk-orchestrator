const { chromium } = require("playwright");
const { spawn } = require("node:child_process");
const { once } = require("node:events");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const assert = require("node:assert/strict");

async function main() {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "risk-stage4-"));
  const root = path.resolve(__dirname, "..");
  const output = process.env.TEST_OUTPUT || path.join(root, "Демонстрация");
  const screenshots = path.join(output, "скриншоты");
  const reports = path.join(output, "протоколы");
  fs.mkdirSync(screenshots, { recursive: true });
  fs.mkdirSync(reports, { recursive: true });
  const server = spawn(
    process.env.TEST_PYTHON || "python",
    [
      "-B",
      "start.py",
      "--db",
      path.join(tmp, "demo.sqlite3"),
      "--port",
      "0",
      "--duration",
      "2",
    ],
    { cwd: root, windowsHide: true, stdio: ["ignore", "pipe", "inherit"] },
  );
  const exited = once(server, "exit");
  let browser;
  try {
    const url = await new Promise((resolve, reject) => {
      let buffer = "";
      server.stdout.on("data", (chunk) => {
        buffer += chunk;
        const address = buffer.match(/http:\/\/127\.0\.0\.1:\d+(?=\s)/);
        if (address) resolve(address[0]);
      });
      server.once("exit", (code) =>
        reject(Error("Сервер остановлен: " + code)),
      );
    });
    browser = await chromium.launch({
      executablePath: process.env.TEST_BROWSER || undefined,
    });
    const page = await browser.newPage({
      viewport: { width: 1366, height: 860 },
    });
    const errors = [];
    page.on("pageerror", (e) => errors.push(e.message));
    await page.goto(url);
    await page.locator('#connection[data-status="online"]').waitFor();
    const snapshot = async () => await (await fetch(url + "/api/state")).json();
    const invalidRun = await page.request.post(url + "/api/run", {
      data: { kind: [], day: "2026-11-22" },
    });
    assert.equal(invalidRun.status(), 400);
    assert.equal((await invalidRun.json()).detail, "Выберите процесс и бизнес-дату.");
    await page.click("#demo");
    await page.getByText("Сделки · ожидает №1").first().waitFor();
    const live = await snapshot();
    assert.deepEqual(
      live.jobs.map((j) => j.status),
      ["running", "running", "queued"],
    );
    assert.deepEqual(live.jobs[0].initiators, ["Анна"]);
    assert.deepEqual(live.jobs[0].subscribers, ["Регламент"]);
    assert.equal(live.jobs[0].request_count, 2);
    await page.click('[data-tab="queue"]');
    await page.getByRole("button",{name:"Сделки · ожидает №1"}).click();
    assert.match(await page.locator("#jobTitle").textContent(), /^RUN-1 ·/);
    const resourceRow = page.locator("#jobBody tr").filter({hasText:"Сделки"});
    assert.match(await resourceRow.first().textContent(), /2026-11-22.*Чтение/);
    await page.locator("#details [data-close]").click();
    await page.screenshot({
      path: path.join(screenshots, "01 — Ожидание ресурса.png"),
    });
    await page.click('[data-tab="timeline"]');
    await page.getByText("v3.1", { exact: true }).waitFor();
    const done = await snapshot();
    const [credit, market, writer] = done.jobs;
    assert(
      Math.max(credit.start, market.start) < Math.min(credit.end, market.end),
    );
    assert(writer.start >= Math.max(credit.end, market.end));
    assert(
      done.jobs.every(
        (j) => j.status === "done" && j.version.rows.length === 8,
      ),
    );
    await page.screenshot({
      path: path.join(screenshots, "02 — Порядок выполнения.png"),
    });
    await page.selectOption("#role", "analyst");
    await page.getByRole("heading", { name: "Панель аналитика" }).waitFor();
    await page.click("#launch");
    await page.click("#launchForm .primary");
    await page.getByText("Ожидает решения", { exact: true }).last().waitFor();
    await page.selectOption("#role", "reviewer");
    await page.locator('[data-job="4"]').first().click();
    assert(await page.locator("#openDemos").isHidden());
    await page.getByText("Проверка синтетических данных",{exact:true}).waitFor();
    await page.click('[data-decision="reject"]');
    await page.getByText("Укажите причину отклонения.").waitFor();
    await page.fill("#comment","Проверены входы и строки версии v4.1");
    await page.click('[data-decision="approve"]');
    await page.waitForFunction(() => !document.querySelector("#details").open);
    assert.equal((await snapshot()).jobs[3].version.approval, "reviewed");
    await page.selectOption("#role", "director");
    await page.locator('[data-job="4"]').first().click();
    await page.fill("#comment", "Проверена версия v4.1");
    assert(await page.locator("#openDemos").isHidden());
    await page.getByText("Комментарий: Проверены входы и строки версии v4.1").waitFor();
    await page.getByRole("button",{name:"Утвердить к публикации"}).waitFor();
    await page.click('[data-decision="approve"]');
    await page.waitForFunction(() => !document.querySelector("#details").open);
    await page.selectOption("#role", "admin");
    await page.click('[data-tab="log"]');
    await page.getByText(/Директор · согласовано v4.1/).waitFor();
    const final = await snapshot();
    assert.equal(final.jobs[3].version.approval, "approved");
    assert.equal(final.jobs[3].status, "done");
    assert.deepEqual(errors, []);
    const report = {
      demoDate: credit.day,
      requests: live.jobs.reduce((total, job) => total + job.request_count, 0),
      executions: done.jobs.length,
      sequence: done.jobs.map((j) => ({
        id: j.id,
        process: j.kind,
        start_s: +(j.start - credit.start).toFixed(3),
        end_s: +(j.end - credit.start).toFixed(3),
        result: j.result,
        values: j.version.rows,
      })),
      approval: final.jobs[3].version.approval,
      events: final.events,
      browserErrors: errors,
    };
    fs.writeFileSync(
      path.join(reports, "Протокол.json"),
      JSON.stringify(report, null, 2),
    );
    console.log(JSON.stringify(report.sequence));
    const scenarios = [];
    for (const [key, requests, count] of [
      ["pipeline", 12, 12], ["capacity", 18, 12], ["reference", 9, 9],
    ]) {
      await page.selectOption("#scenario", key);
      const created = page.waitForResponse(r => r.url().includes("/api/demo?scenario="+key));
      await page.click("#demo");
      const batch = await (await created).json();
      assert.equal(batch.jobs.length,count);
      assert.equal(batch.requests,requests);
      await page.getByRole("heading",{name:{pipeline:"Закрытие трёх дат",capacity:"Пиковая нагрузка",reference:"Общий справочник"}[key],exact:true}).waitFor();
      await page.waitForFunction(n => document.querySelector(".batchstats")?.textContent.includes(n+"/"+n), count, {timeout:60000});
      const completed = await snapshot();
      const jobs = completed.jobs.filter(j=>batch.jobs.includes(j.id));
      assert(jobs.every(j=>j.status==="done" && j.result));
      const events=completed.events.filter(e=>batch.jobs.includes(e.job));
      for(const j of jobs.filter(j=>j.parent)){
        assert(j.start>=jobs.find(p=>p.id===j.parent).end);
      }
      if(key==="capacity") {
        assert(events.some(e=>e.message==="Все исполнители заняты"));
        assert.equal(Math.max(...jobs.map(j=>jobs.filter(p=>p.start<=j.start && j.start<p.end).length)),4);
      }
      if(key==="reference"){
        const ref=jobs.find(j=>j.kind==="FX");
        assert(events.some(e=>e.job===ref.id && e.message.includes("Справочник · ожидает")));
        assert(jobs.filter(j=>j.parent===ref.id).every(j=>j.inputs.reference===ref.result));
      }
      await page.screenshot({path:path.join(screenshots,key+".png"),fullPage:true});
      scenarios.push({scenario:key,requests,runs:count,events});
    }
    await page.selectOption("#batch","1");
    await page.getByRole("heading",{name:"Чтение и запись"}).waitFor();
    await page.selectOption("#scenario","reapproval");
    const launched = page.waitForResponse(r=>r.url().includes("/api/demo?scenario=reapproval"));
    await page.click("#demo");
    const reviewBatch = await (await launched).json();
    const [original, reference] = reviewBatch.jobs;
    const oldRun = page.locator(`#view [data-job="${original}"]`).first();
    await oldRun.waitFor();
    let resumeSwitch;
    const holdSwitch = new Promise(resolve => { resumeSwitch = resolve; });
    await page.route("**/api/role/reviewer", async route => {
      await holdSwitch;
      await route.continue();
    }, { times: 1 });
    await page.selectOption("#role","reviewer");
    try {
      const bounds = await oldRun.boundingBox();
      assert(bounds);
      await page.mouse.click(bounds.x + bounds.width / 2, bounds.y + bounds.height / 2);
      assert.equal(await page.locator("#details").evaluate(dialog => dialog.open), false,
        "A run from the previous role must not open during a role switch");
    } finally {
      resumeSwitch();
    }
    await page.locator('[data-tab="review"][aria-current="page"]').waitFor();
    await page.locator(`[data-job="${original}"]`).first().click();
    await page.getByText(new RegExp("Изменился вход «Справочник».*v"+reference+"\\.1")).waitFor();
    assert(await page.locator('[data-decision="approve"]').isDisabled());
    await page.fill("#comment","Нужна повторная проверка");
    const decision = page.waitForResponse(r=>r.url().endsWith("/decision"));
    await page.click('[data-decision="reject"]');
    const {successor} = await (await decision).json();
    await page.getByText(`RUN-${original} возвращён на перерасчёт. Создан RUN-${successor}.`).waitFor();
    await page.locator(`[data-job="${successor}"]`).first().click();
    await page.click('[data-decision="approve"]');
    await page.waitForFunction(()=>!document.querySelector("#details").open);
    await page.selectOption("#role","director");
    await page.locator(`[data-job="${successor}"]`).first().click();
    await page.click('[data-decision="approve"]');
    await page.waitForFunction(()=>!document.querySelector("#details").open);
    await page.selectOption("#role","admin");
    await page.locator(`[data-job="${original}"]`).first().click();
    await page.locator(`#jobBody [data-job="${successor}"]`).click();
    await page.locator(`#jobBody [data-job="${original}"]`).click();
    assert((await page.locator("#jobTitle").textContent()).startsWith("RUN-"+original+" ·"));
    const state = await snapshot();
    const batch = state.batches.find(b=>b.id===reviewBatch.id);
    assert.deepEqual(batch.jobs,[original,reference,successor]);
    const published = state.jobs.find(j=>j.id===successor);
    assert.equal(published.version.approval,"approved");
    assert.equal(published.inputs.reference,state.jobs.find(j=>j.id===reference).result);
    scenarios.push({scenario:"reapproval",requests:batch.requests,runs:batch.jobs.length,
      events:state.events.filter(e=>batch.jobs.includes(e.job))});
    assert.deepEqual(errors, []);
    fs.writeFileSync(path.join(reports,"Сценарии.json"),JSON.stringify(scenarios,null,2));
  } finally {
    if (browser) await browser.close();
    server.kill();
    await exited;
  }
}
main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
