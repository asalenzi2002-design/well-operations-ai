import React, { useEffect, useMemo, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  PieChart,
  Pie,
  Cell,
  LineChart,
  Line,
} from "recharts";
import {
  Activity,
  AlertTriangle,
  ArrowDown,
  CheckCircle2,
  Gauge,
  Loader2,
  MapPinned,
  Search,
  ShieldAlert,
  Target,
  TrendingDown,
  TrendingUp,
  Wrench,
} from "lucide-react";

const API_BASE = "http://localhost:3002";

const fallbackOverview = {
  total_rate: 78337,
  hourly_average: 3264,
  daily_average: 78337,
  monthly_target: 85000,
  target_gap: 6663,
  active_dn_count: 18,
  well_status_counts: {
    on_production: 90,
    shut_in: 12,
    testing: 8,
    mothball: 4,
    standby: 3,
    locked_potential: 3,
  },
  field_contribution: [
    { field: "Ain Dar", value: 45200 },
    { field: "Abqaiq", value: 33137 },
  ],
  smart_blocks: {
    production_trend: { direction: "down", value: -2.8, label: "vs yesterday" },
    dn_impact: { potential_loss_bopd: 5120, top_dn: "DN-1042" },
    performance: { status: "Below Target", gap: 6663 },
  },
  insights: [
    "Production is below target and needs recovery actions.",
    "DN backlog is starting to hit operational output.",
    "Ain Dar is carrying more load; dependency risk is rising.",
  ],
};

const fallbackRisk = {
  top_risk_wells: [
    {
      well_name: "ANDR-1550",
      production_status: "Locked Potential",
      oil_rate_bopd: 0,
      risk_score: 86,
      risk_level: "HIGH",
      reasons: ["Locked potential", "High-priority overdue DN"],
    },
    {
      well_name: "ABQQ-1511",
      production_status: "Shut-in",
      oil_rate_bopd: 0,
      risk_score: 78,
      risk_level: "HIGH",
      reasons: ["Shut-in with open DN", "Aging work item"],
    },
    {
      well_name: "ANDR-234",
      production_status: "Testing",
      oil_rate_bopd: 920,
      risk_score: 62,
      risk_level: "MEDIUM",
      reasons: ["Unstable production state"],
    },
  ],
};

const fallbackIntelligence = {
  generated_at: new Date().toISOString(),
  production: {
    total_rate_bopd: 78337,
    producing_well_count: 90,
    shut_in_well_count: 12,
    locked_potential_count: 3,
    field_breakdown: [
      { field_code: "ANDR", total_rate_bopd: 45200, well_count: 58 },
      { field_code: "ABQQ", total_rate_bopd: 33137, well_count: 62 },
    ],
  },
  dn: {
    active_count: 18,
    overdue_count: 7,
    high_priority_count: 5,
    blocked_count: 3,
    top_items: [
      { dn_id: "DN-1042", well_name: "ANDR-1550", owner: "CRD", priority: "High", progress_percent: 45 },
      { dn_id: "DN-1071", well_name: "ABQQ-1511", owner: "CFC", priority: "High", progress_percent: 30 },
      { dn_id: "DN-1080", well_name: "ANDR-411", owner: "Inspection", priority: "Medium", progress_percent: 70 },
    ],
  },
  risk: {
    highest_risk_field: "ANDR",
    average_risk_score: 54,
    high_risk_count: 6,
  },
  drop: {
    total_drop_bopd: 2280,
    biggest_drop_well: "ABQQ-1511",
    biggest_drop_value: 940,
  },
  flags: [
    "Aging DN cluster in Ain Dar",
    "Target recovery needed within current operating window",
    "Locked potential wells need focused clearance",
  ],
  summary: "System is stable but under target. DN pressure and locked potential are the main operational drag right now.",
};

const fallbackSummary = {
  total_rate: 78337,
  hourly_average: 3264,
  daily_average: 78337,
};

function formatNumber(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  return new Intl.NumberFormat().format(Number(value));
}

function getStatusTone(status) {
  const s = String(status || "").toLowerCase();
  if (s.includes("high") || s.includes("shut") || s.includes("overdue")) return "destructive";
  if (s.includes("medium") || s.includes("testing") || s.includes("locked")) return "secondary";
  return "default";
}

function normalizeOverview(raw) {
  if (!raw || typeof raw !== "object") return fallbackOverview;
  return {
    ...fallbackOverview,
    ...raw,
    well_status_counts: {
      ...fallbackOverview.well_status_counts,
      ...(raw.well_status_counts || {}),
    },
    field_contribution: raw.field_contribution || fallbackOverview.field_contribution,
    smart_blocks: {
      ...fallbackOverview.smart_blocks,
      ...(raw.smart_blocks || {}),
    },
    insights: raw.insights || fallbackOverview.insights,
  };
}

async function fetchJson(path) {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) throw new Error(`Failed ${path}: ${res.status}`);
  return res.json();
}

function SectionTitle({ icon: Icon, title, subtitle }) {
  return (
    <div className="flex items-start justify-between gap-4">
      <div className="flex items-start gap-3">
        <div className="rounded-2xl bg-muted p-3">
          <Icon className="h-5 w-5" />
        </div>
        <div>
          <h2 className="text-xl font-semibold tracking-tight">{title}</h2>
          <p className="text-sm text-muted-foreground">{subtitle}</p>
        </div>
      </div>
    </div>
  );
}

function KPI({ title, value, hint, icon: Icon }) {
  return (
    <Card className="rounded-2xl shadow-sm">
      <CardContent className="p-5">
        <div className="flex items-start justify-between">
          <div>
            <p className="text-sm text-muted-foreground">{title}</p>
            <p className="mt-2 text-3xl font-bold tracking-tight">{value}</p>
            <p className="mt-2 text-xs text-muted-foreground">{hint}</p>
          </div>
          <div className="rounded-2xl bg-muted p-3">
            <Icon className="h-5 w-5" />
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

export default function WellOperationsDashboard() {
  const [overview, setOverview] = useState(fallbackOverview);
  const [risk, setRisk] = useState(fallbackRisk);
  const [intelligence, setIntelligence] = useState(fallbackIntelligence);
  const [summary, setSummary] = useState(fallbackSummary);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState("");
  const [wellSearch, setWellSearch] = useState("");

  const loadData = async (silent = false) => {
    try {
      if (silent) setRefreshing(true);
      else setLoading(true);
      setError("");

      const [overviewData, riskData, intelligenceData, summaryData] = await Promise.all([
        fetchJson("/dashboard/overview").catch(() => fallbackOverview),
        fetchJson("/dashboard/risk").catch(() => fallbackRisk),
        fetchJson("/dashboard/intelligence").catch(() => fallbackIntelligence),
        fetchJson("/dashboard/summary").catch(() => fallbackSummary),
      ]);

      setOverview(normalizeOverview(overviewData));
      setRisk(riskData || fallbackRisk);
      setIntelligence(intelligenceData || fallbackIntelligence);
      setSummary(summaryData || fallbackSummary);
    } catch (err) {
      setError(err.message || "Failed to load dashboard data.");
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => {
    loadData();
  }, []);

  const statusChartData = useMemo(() => {
    const counts = overview.well_status_counts || {};
    return [
      { name: "Production", value: counts.on_production || 0 },
      { name: "Shut-in", value: counts.shut_in || 0 },
      { name: "Testing", value: counts.testing || 0 },
      { name: "Mothball", value: counts.mothball || 0 },
      { name: "Standby", value: counts.standby || 0 },
      { name: "Locked", value: counts.locked_potential || 0 },
    ];
  }, [overview]);

  const fieldData = useMemo(() => {
    const source = overview.field_contribution || [];
    return source.map((item) => ({
      name: item.field || item.field_code || "Unknown",
      value: item.value || item.total_rate_bopd || 0,
    }));
  }, [overview]);

  const filteredRiskWells = useMemo(() => {
    const items = risk?.top_risk_wells || [];
    const q = wellSearch.trim().toLowerCase();
    if (!q) return items;
    return items.filter((item) =>
      [item.well_name, item.production_status, ...(item.reasons || [])]
        .join(" ")
        .toLowerCase()
        .includes(q)
    );
  }, [risk, wellSearch]);

  const trendBlocks = useMemo(() => {
    const block = overview.smart_blocks?.production_trend || {};
    return [
      { day: "D-6", value: 79600 },
      { day: "D-5", value: 80120 },
      { day: "D-4", value: 79440 },
      { day: "D-3", value: 78810 },
      { day: "D-2", value: 79020 },
      { day: "D-1", value: 80580 },
      { day: "Today", value: summary.total_rate || 78337 },
      { delta: block.value || 0 },
    ].filter((x) => x.day);
  }, [overview, summary]);

  if (loading) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-background p-6">
        <div className="flex items-center gap-3 rounded-2xl border bg-card px-5 py-4 shadow-sm">
          <Loader2 className="h-5 w-5 animate-spin" />
          <span className="text-sm text-muted-foreground">Loading Well Operations UI...</span>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-background p-4 md:p-8">
      <div className="mx-auto max-w-7xl space-y-8">
        <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
          <div>
            <p className="text-sm font-medium text-muted-foreground">Well Operations AI</p>
            <h1 className="text-3xl font-bold tracking-tight md:text-4xl">Operational Dashboard</h1>
            <p className="mt-2 max-w-3xl text-sm text-muted-foreground">
              High-level field view first, then drill-down into production, locked potential, DN pressure,
              risk, and field distribution.
            </p>
          </div>

          <div className="flex flex-wrap items-center gap-3">
            <Badge variant="outline" className="rounded-xl px-3 py-1 text-xs">
              Generated: {new Date(intelligence.generated_at || Date.now()).toLocaleString()}
            </Badge>
            <Button className="rounded-2xl" onClick={() => loadData(true)}>
              {refreshing ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Activity className="mr-2 h-4 w-4" />}
              Refresh
            </Button>
          </div>
        </div>

        {error ? (
          <Alert className="rounded-2xl border-destructive/40">
            <AlertTriangle className="h-4 w-4" />
            <AlertTitle>Backend issue</AlertTitle>
            <AlertDescription>
              {error}. The screen is showing fallback demo data so you can still continue UI work.
            </AlertDescription>
          </Alert>
        ) : null}

        <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
          <KPI title="Total Rate" value={`${formatNumber(summary.total_rate || overview.total_rate)} BOPD`} hint="Current production output" icon={Gauge} />
          <KPI title="Daily Average" value={`${formatNumber(summary.daily_average || overview.daily_average)} BOPD`} hint="24-hour average" icon={TrendingUp} />
          <KPI title="Monthly Target" value={`${formatNumber(overview.monthly_target)} BOPD`} hint="Configured target" icon={Target} />
          <KPI title="Target Gap" value={`${formatNumber(overview.target_gap)} BOPD`} hint="Difference from target" icon={TrendingDown} />
          <KPI title="Active DNs" value={formatNumber(overview.active_dn_count)} hint="Open work items impacting flow" icon={Wrench} />
        </section>

        <section className="grid gap-4 xl:grid-cols-3">
          <Card className="rounded-2xl xl:col-span-2">
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-lg">
                <Activity className="h-5 w-5" /> Overview Signal
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid gap-3 md:grid-cols-3">
                <div className="rounded-2xl border p-4">
                  <p className="text-sm text-muted-foreground">Performance</p>
                  <p className="mt-2 text-xl font-semibold">{overview.smart_blocks?.performance?.status || "Unknown"}</p>
                  <p className="mt-1 text-sm text-muted-foreground">
                    Gap: {formatNumber(overview.smart_blocks?.performance?.gap || 0)} BOPD
                  </p>
                </div>
                <div className="rounded-2xl border p-4">
                  <p className="text-sm text-muted-foreground">DN Impact</p>
                  <p className="mt-2 text-xl font-semibold">
                    {formatNumber(overview.smart_blocks?.dn_impact?.potential_loss_bopd || 0)} BOPD
                  </p>
                  <p className="mt-1 text-sm text-muted-foreground">
                    Top DN: {overview.smart_blocks?.dn_impact?.top_dn || "—"}
                  </p>
                </div>
                <div className="rounded-2xl border p-4">
                  <p className="text-sm text-muted-foreground">Short Trend</p>
                  <p className="mt-2 flex items-center gap-2 text-xl font-semibold">
                    {(overview.smart_blocks?.production_trend?.direction || "down") === "up" ? (
                      <TrendingUp className="h-5 w-5" />
                    ) : (
                      <ArrowDown className="h-5 w-5" />
                    )}
                    {overview.smart_blocks?.production_trend?.value || 0}%
                  </p>
                  <p className="mt-1 text-sm text-muted-foreground">
                    {overview.smart_blocks?.production_trend?.label || "vs previous period"}
                  </p>
                </div>
              </div>

              <div className="rounded-2xl border p-4">
                <p className="mb-3 text-sm font-medium">Executive Insight</p>
                <p className="text-sm leading-6 text-muted-foreground">{intelligence.summary}</p>
              </div>
            </CardContent>
          </Card>

          <Card className="rounded-2xl">
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-lg">
                <ShieldAlert className="h-5 w-5" /> Active Flags
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              {(intelligence.flags || []).map((flag, index) => (
                <div key={index} className="rounded-2xl border p-3 text-sm">
                  {flag}
                </div>
              ))}
            </CardContent>
          </Card>
        </section>

        <Tabs defaultValue="production" className="space-y-6">
          <TabsList className="grid w-full grid-cols-2 gap-2 rounded-2xl bg-muted p-2 md:grid-cols-6">
            <TabsTrigger value="production" className="rounded-xl">Production</TabsTrigger>
            <TabsTrigger value="locked" className="rounded-xl">Locked Potential</TabsTrigger>
            <TabsTrigger value="dn" className="rounded-xl">DN</TabsTrigger>
            <TabsTrigger value="hips" className="rounded-xl">HIPS</TabsTrigger>
            <TabsTrigger value="formation" className="rounded-xl">Formation Line</TabsTrigger>
            <TabsTrigger value="field" className="rounded-xl">Field View</TabsTrigger>
          </TabsList>

          <TabsContent value="production" className="space-y-6">
            <SectionTitle icon={Gauge} title="Production Status" subtitle="Top-level production picture with trend and status spread." />
            <div className="grid gap-4 xl:grid-cols-3">
              <Card className="rounded-2xl xl:col-span-2">
                <CardHeader>
                  <CardTitle>Production Trend</CardTitle>
                </CardHeader>
                <CardContent className="h-[320px]">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={trendBlocks}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="day" />
                      <YAxis />
                      <Tooltip />
                      <Line type="monotone" dataKey="value" strokeWidth={3} />
                    </LineChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>

              <Card className="rounded-2xl">
                <CardHeader>
                  <CardTitle>Well Status Mix</CardTitle>
                </CardHeader>
                <CardContent className="h-[320px]">
                  <ResponsiveContainer width="100%" height="100%">
                    <PieChart>
                      <Pie data={statusChartData} dataKey="value" nameKey="name" outerRadius={100} label>
                        {statusChartData.map((_, index) => (
                          <Cell key={index} />
                        ))}
                      </Pie>
                      <Tooltip />
                    </PieChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>
            </div>
          </TabsContent>

          <TabsContent value="locked" className="space-y-6">
            <SectionTitle icon={AlertTriangle} title="Locked Potential" subtitle="Wells blocked from value despite recoverable potential." />
            <div className="grid gap-4 lg:grid-cols-3">
              <KPI title="Locked Wells" value={formatNumber(overview.well_status_counts?.locked_potential || intelligence.production?.locked_potential_count || 0)} hint="Current locked potential count" icon={AlertTriangle} />
              <KPI title="Highest Risk Field" value={intelligence.risk?.highest_risk_field || "—"} hint="Most pressured field right now" icon={MapPinned} />
              <KPI title="High Risk Wells" value={formatNumber(intelligence.risk?.high_risk_count || 0)} hint="Needs fast intervention" icon={ShieldAlert} />
            </div>

            <Card className="rounded-2xl">
              <CardHeader>
                <CardTitle>Top Risk Wells</CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="relative max-w-md">
                  <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                  <Input
                    value={wellSearch}
                    onChange={(e) => setWellSearch(e.target.value)}
                    placeholder="Search wells, status, or reasons..."
                    className="rounded-2xl pl-9"
                  />
                </div>

                <div className="grid gap-3">
                  {filteredRiskWells.map((well, index) => (
                    <div key={`${well.well_name}-${index}`} className="rounded-2xl border p-4">
                      <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
                        <div>
                          <div className="flex items-center gap-2">
                            <p className="text-lg font-semibold">{well.well_name}</p>
                            <Badge variant={getStatusTone(well.risk_level)}>{well.risk_level}</Badge>
                          </div>
                          <p className="mt-1 text-sm text-muted-foreground">
                            Status: {well.production_status} • Rate: {formatNumber(well.oil_rate_bopd)} BOPD
                          </p>
                        </div>
                        <div className="text-right">
                          <p className="text-sm text-muted-foreground">Risk Score</p>
                          <p className="text-2xl font-bold">{formatNumber(well.risk_score)}</p>
                        </div>
                      </div>
                      <div className="mt-3 flex flex-wrap gap-2">
                        {(well.reasons || []).map((reason, i) => (
                          <Badge key={i} variant="outline" className="rounded-xl">{reason}</Badge>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="dn" className="space-y-6">
            <SectionTitle icon={Wrench} title="DN Workflow" subtitle="Backlog pressure, top items, and ownership visibility." />
            <div className="grid gap-4 md:grid-cols-4">
              <KPI title="Active DNs" value={formatNumber(intelligence.dn?.active_count || 0)} hint="Currently open" icon={Wrench} />
              <KPI title="Overdue" value={formatNumber(intelligence.dn?.overdue_count || 0)} hint="Past KPI window" icon={AlertTriangle} />
              <KPI title="High Priority" value={formatNumber(intelligence.dn?.high_priority_count || 0)} hint="Priority focus" icon={ShieldAlert} />
              <KPI title="Blocked" value={formatNumber(intelligence.dn?.blocked_count || 0)} hint="Progress constraint" icon={TrendingDown} />
            </div>

            <Card className="rounded-2xl">
              <CardHeader>
                <CardTitle>Top DN Items</CardTitle>
              </CardHeader>
              <CardContent>
                <ScrollArea className="h-[340px] pr-3">
                  <div className="space-y-3">
                    {(intelligence.dn?.top_items || []).map((item, index) => (
                      <div key={`${item.dn_id}-${index}`} className="rounded-2xl border p-4">
                        <div className="flex flex-col gap-2 md:flex-row md:items-center md:justify-between">
                          <div>
                            <p className="font-semibold">{item.dn_id}</p>
                            <p className="text-sm text-muted-foreground">
                              {item.well_name} • Owner: {item.owner} • Priority: {item.priority}
                            </p>
                          </div>
                          <Badge variant={getStatusTone(item.priority)}>{item.progress_percent || 0}% progress</Badge>
                        </div>
                      </div>
                    ))}
                  </div>
                </ScrollArea>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="hips" className="space-y-6">
            <SectionTitle icon={CheckCircle2} title="HIPS Installation" subtitle="Reserved UI block for the separate HIPS workflow, independent from DN." />
            <Card className="rounded-2xl">
              <CardContent className="p-6">
                <div className="rounded-2xl border border-dashed p-8 text-sm text-muted-foreground">
                  This section is intentionally separated because HIPS is its own engineering-driven workflow. Next step is to connect a dedicated backend endpoint or mock dataset for FOEU initiation, field preparation, CRD execution, and completion tracking.
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="formation" className="space-y-6">
            <SectionTitle icon={CheckCircle2} title="New Formation Line" subtitle="Standalone workflow block, not mixed with DN or HIPS." />
            <Card className="rounded-2xl">
              <CardContent className="p-6">
                <div className="rounded-2xl border border-dashed p-8 text-sm text-muted-foreground">
                  This block is ready for a dedicated workflow card list, milestones, aging tracker, and ownership model once the Formation Line backend dataset is added.
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="field" className="space-y-6">
            <SectionTitle icon={MapPinned} title="Field View" subtitle="Contribution split and field-level load visibility." />
            <div className="grid gap-4 xl:grid-cols-3">
              <Card className="rounded-2xl xl:col-span-2">
                <CardHeader>
                  <CardTitle>Field Contribution</CardTitle>
                </CardHeader>
                <CardContent className="h-[320px]">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={fieldData}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="name" />
                      <YAxis />
                      <Tooltip />
                      <Bar dataKey="value" radius={[10, 10, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </CardContent>
              </Card>

              <Card className="rounded-2xl">
                <CardHeader>
                  <CardTitle>Field Breakdown</CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  {(intelligence.production?.field_breakdown || []).map((item, index) => (
                    <div key={`${item.field_code}-${index}`} className="rounded-2xl border p-4">
                      <div className="flex items-center justify-between">
                        <p className="font-semibold">{item.field_code}</p>
                        <Badge variant="outline">{formatNumber(item.well_count)} wells</Badge>
                      </div>
                      <p className="mt-2 text-sm text-muted-foreground">
                        {formatNumber(item.total_rate_bopd)} BOPD total contribution
                      </p>
                    </div>
                  ))}
                </CardContent>
              </Card>
            </div>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}
