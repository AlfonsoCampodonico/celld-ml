// Interleaving, transactions, the Cache API, HTMLRewriter, Node.js
// modules, and a stateless worker reached through a service binding.
// Deploy: curl -X PUT --data-binary @examples/edge.js http://10.0.0.11:8080/.well-known/celld/classes/Edge
const { Buffer } = require("node:buffer");
const WASM = [0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x0e, 0x03, 0x60, 0x02, 0x7f, 0x7f, 0x01, 0x7f, 0x60, 0x01, 0x7f, 0x00, 0x60, 0x00, 0x00, 0x02, 0x0b, 0x01, 0x03, 0x65, 0x6e, 0x76, 0x03, 0x6c, 0x6f, 0x67, 0x00, 0x01, 0x03, 0x04, 0x03, 0x00, 0x01, 0x02, 0x05, 0x03, 0x01, 0x00, 0x01, 0x07, 0x21, 0x04, 0x03, 0x61, 0x64, 0x64, 0x00, 0x01, 0x07, 0x63, 0x61, 0x6c, 0x6c, 0x4c, 0x6f, 0x67, 0x00, 0x02, 0x04, 0x73, 0x70, 0x69, 0x6e, 0x00, 0x03, 0x06, 0x6d, 0x65, 0x6d, 0x6f, 0x72, 0x79, 0x02, 0x00, 0x0a, 0x1b, 0x03, 0x07, 0x00, 0x20, 0x00, 0x20, 0x01, 0x6a, 0x0b, 0x09, 0x00, 0x20, 0x00, 0x41, 0x01, 0x6a, 0x10, 0x00, 0x0b, 0x07, 0x00, 0x03, 0x40, 0x0c, 0x00, 0x0b, 0x0b, 0x0b, 0x08, 0x01, 0x00, 0x41, 0x00, 0x0b, 0x02, 0x68, 0x69];

async function fetch(request, ctx) {
  const url = new URL(request.url);
  const p = url.pathname;
  // On /w/Edge/... this class runs as a stateless worker: no cell, no storage.
  if (p === "/hello") return new Response("hello from a stateless worker, " + Buffer.from(url.searchParams.get("to") || "", "utf8").toString("base64") + "\n");
  if (p === "/slow") { await new Promise((r) => setTimeout(r, 2000)); return new Response("slow done\n"); }
  if (p === "/fast") return new Response("fast\n");
  if (p === "/throw") { ctx.storage.put("before_throw", true); throw new Error("boom"); }
  if (p === "/tx") {
    try { ctx.storage.transactionSync(() => { ctx.storage.put("in_tx", true); throw new Error("abort"); }); } catch (e) {}
    return Response.json({ kept_after_throw: ctx.storage.get("before_throw") === true, rolled_back: ctx.storage.get("in_tx") === undefined });
  }
  if (p === "/cached") {
    const key = new Request("https://cache.local/answer");
    const hit = await caches.default.match(key);
    if (hit) return new Response("cache hit: " + (await hit.text()));
    await caches.default.put(key, new Response("42\n", { headers: { "cache-control": "max-age=60" } }));
    return new Response("cache miss, stored\n");
  }
  if (p === "/service") return await ctx.service("Edge").fetch("/hello?to=cell");
  if (p === "/wasm") {
    // a tiny module: add(i32, i32), callLog(i32) -> env.log(x + 1), spin() loops forever, memory with "hi"
    const bytes = new Uint8Array(WASM);
    const logged = [];
    const { instance } = await WebAssembly.instantiate(bytes, { env: { log: (x) => logged.push(x) } });
    instance.exports.callLog(9);
    const mem = new Uint8Array(instance.exports.memory.buffer);
    let spin;
    try { instance.exports.spin(); } catch (e) { spin = e.name; }
    return Response.json({ add: instance.exports.add(20, 22), logged, data: String.fromCharCode(mem[0], mem[1]), spin });
  }
  if (p === "/html")
    return new HTMLRewriter()
      .on("a", { element(e) { e.setAttribute("rel", "noopener"); } })
      .transform(new Response('<p>see <a href="/x">x</a></p>', { headers: { "content-type": "text/html" } }));
  return new Response("not found\n", { status: 404 });
}
