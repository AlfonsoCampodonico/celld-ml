// A tour of the Workers APIs celld-ml runs.
// Deploy: curl -X PUT --data-binary @examples/workers.js http://10.0.0.11:8080/.well-known/celld/classes/Api
const VERSION = 1;

async function fetch(request, ctx) {
  const url = new URL(request.url);
  const p = url.pathname;
  if (p === "/version") return new Response("version " + VERSION + "\n");
  if (p === "/fetch") {
    const r = await fetch("http://upstream.test:8099/hello?from=" + ctx.id, { headers: { "x-test": "1" } });
    return Response.json({ status: r.status, upstream: await r.json() });
  }
  if (p === "/forbidden") {
    try { await fetch("http://10.0.0.2:9000/list?prefix="); return new Response("reached the bucket!\n", { status: 500 }); }
    catch (e) { return new Response("refused: " + e.message + "\n"); }
  }
  if (p === "/crypto") {
    const d = new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode("celld")));
    return Response.json({ sha256: [...d].map((b) => b.toString(16).padStart(2, "0")).join(""), uuid: crypto.randomUUID() });
  }
  if (p === "/d1") {
    const db = ctx.d1("main");
    await db.batch([
      db.prepare("CREATE TABLE IF NOT EXISTS visits(id INTEGER PRIMARY KEY, who TEXT, n REAL)"),
      db.prepare("INSERT INTO visits(who, n) VALUES (?, ?)").bind(ctx.id, 1.5),
    ]);
    const { results } = await db.prepare("SELECT who, count(*) AS c, sum(n) AS total FROM visits GROUP BY who").all();
    return Response.json(results);
  }
  if (p.startsWith("/r2")) {
    const bucket = ctx.r2("files"), key = p.slice(4);
    if (request.method === "PUT") { const o = await bucket.put(key, await request.text(), { customMetadata: { by: ctx.id } }); return Response.json({ etag: o.etag, size: o.size }); }
    if (key) { const o = await bucket.get(key); return o ? new Response((await o.text()) + " (by " + o.customMetadata.by + ")\n") : new Response("missing\n", { status: 404 }); }
    return Response.json((await bucket.list()).objects.map((o) => o.key));
  }
  if (p === "/stub") {
    const counter = await ctx.cell("Counter:shared").fetch("/increment", { method: "POST" });
    const other = await ctx.cell("Api:other").fetch("/version");
    return new Response("counter " + (await counter.text()).trim() + ", other cell says " + (await other.text()));
  }
  if (p === "/timer") {
    const t0 = Date.now();
    await new Promise((r) => setTimeout(r, 300));
    return new Response("slept " + (Date.now() - t0 >= 290) + "\n");
  }
  if (p === "/wait") {
    ctx.waitUntil((async () => { await new Promise((r) => setTimeout(r, 100)); ctx.storage.put("waited", true); })());
    return new Response("accepted\n");
  }
  if (p === "/waited") return new Response(String(ctx.storage.get("waited")) + "\n");
  if (p === "/wf" && request.method === "POST") {
    const wf = await ctx.workflow("Api").create({ params: await request.json() });
    return Response.json({ id: wf.id });
  }
  if (p.startsWith("/wf/")) return Response.json(await (await ctx.workflow("Api").get(p.slice(4))).status());
  return new Response("not found\n", { status: 404 });
}

// A workflow: steps are memoized; a sleep or a failed step suspends the run
// and an alarm resumes it.
async function run(event, step, ctx) {
  const up = await step.do("ask upstream", async () => (await (await fetch("http://upstream.test:8099/step")).json()).path);
  await step.sleep("nap", "2 seconds");
  // fails on its first attempt; its writes are rolled back each time
  const flaky = await step.do("flaky", { retries: { limit: 3, delay: "1 second", backoff: "constant" } }, async ({ attempt }) => {
    if (attempt < 2) throw new Error("flaky on attempt " + attempt);
    return "ok on attempt " + attempt;
  });
  return { up, flaky, n: event.payload.n * 2 };
}
