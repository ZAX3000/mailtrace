import { defineStore } from "pinia";
import { getRunResult, type KPIs } from "@/api/result";

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
    async ensureRun(runId?: string): Promise<boolean> {
      if (runId) this.runId = runId;
      return !!this.runId;
    },

    async fetch(runId?: string) {
      const hasId = await this.ensureRun(runId);
      if (!hasId) { 
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
        const status = e?.response?.status ?? e?.status;
        const msg = e?.response?.data?.error || e?.message;
        if (status === 409 || msg === "not_ready") {
          this.error = null;
          this.kpis = null;
        } else {
          this.error = msg || "Unable to load KPIs";
        }
      } finally {
        this.loading = false;
      }
    }
  }
});