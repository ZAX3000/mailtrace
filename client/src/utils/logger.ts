const DEBUG = true; // flip off in prod

function stamp() {
  const d = new Date();
  return d.toISOString().split('T')[1]?.replace('Z','') ?? '';
}
export type Ctx = Record<string, any>;

function fmt(ctx?: Ctx) {
  return ctx ? ' ' + JSON.stringify(ctx) : '';
}

export const log = {
  debug(msg: string, ctx?: Ctx) { if (DEBUG) console.debug(`[${stamp()}] 🟦 ${msg}${fmt(ctx)}`); },
  info (msg: string, ctx?: Ctx) { if (DEBUG) console.info (`[${stamp()}] 🟩 ${msg}${fmt(ctx)}`); },
  warn (msg: string, ctx?: Ctx) { if (DEBUG) console.warn (`[${stamp()}] 🟨 ${msg}${fmt(ctx)}`); },
  error(msg: string, ctx?: Ctx) { console.error(`[${stamp()}] 🟥 ${msg}${fmt(ctx)}`); },
};

// simple correlation id for each kickoff; store it on window so all logs tag it
export function newReqId() {
  return Math.random().toString(36).slice(2) + '-' + Date.now().toString(36);
}
export function setReq(reqId: string) {
  (window as any).__REQ_ID__ = reqId;
}
export function getReq() {
  return (window as any).__REQ_ID__ as string | undefined;
}