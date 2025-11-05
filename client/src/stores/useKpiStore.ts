import { defineStore } from "pinia";
import { getRunResult, type KPIs } from "@/api/result"; // remove createRun import

type State = {
  runId: string | null;
  loading: boolean;
  error: string | null;
  kpis: KPIs | null;
};

export const useKpiStore = defineStore("kpi", {
  state: (): State => ({
    runId: null,
    loading: false,
    error: null,
    kpis: null,
  }),
  actions: {
    // Only accept an existing run id (from props/SSE). Don't create.
    async ensureRun(runId?: string): Promise<boolean> {
      if (runId) this.runId = runId;
      return !!this.runId;
    },

    async fetch(runId?: string) {
      const hasId = await this.ensureRun(runId);
      if (!hasId) { 
        // nothing to do; keep the card empty without error
        this.kpis = null;
        this.error = null;
        return;
      }

      this.loading = true; 
      this.error = null;

      try {
        const res = await getRunResult(this.runId!);
        this.kpis = res.kpis || {};
      } catch (e: any) {
        // If backend returns 409 not_ready, treat it as "not ready yet", not as an error.
        const status = e?.response?.status ?? e?.status;
        const msg = e?.response?.data?.error || e?.message;
        if (status === 409 || msg === "not_ready") {
          this.error = null;     // don't show warning UI
          this.kpis = null;      // keep it blank
        } else {
          this.error = msg || "Unable to load KPIs";
        }
      } finally {
        this.loading = false;
      }
    }
  }
});