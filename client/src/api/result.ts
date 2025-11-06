import { http } from "./http";

export async function createRun(): Promise<string> {
  const { data } = await http.post("/runs");
  return data.run_id as string;
}

export type KPIs = {
  total_mail?: number;
  unique_mail_addresses?: number;
  total_jobs?: number;
  matches?: number;
  match_rate?: number;       // 0..100 or 0..1 depending on backend — your DAO returns float %
  match_revenue?: number;    // dollars
  revenue_per_mailer?: number;
  avg_ticket_per_match?: number;
  median_days_to_convert?: number;
};

export type RunResult = {
  run_id: string;
  kpis: KPIs;
  // graph/top_cities/top_zips also returned but unused in this demo
};

export async function getRunResult(runId: string): Promise<RunResult> {
  const { data } = await http.get(`/runs/${runId}/result`);
  return data as RunResult;
}