import React from "react";
import { X, AlertTriangle, Gauge, Wrench } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";

function formatNumber(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  return new Intl.NumberFormat().format(Number(value));
}

function deriveFieldCode(value) {
  const raw = String(value || "").toUpperCase();
  if (raw.includes("ANDR") || raw.includes("AIN DAR")) return "ANDR";
  if (raw.includes("ABQQ") || raw.includes("ABQAIQ")) return "ABQQ";
  return "UNKNOWN";
}

function getFieldLabel(value) {
  const code = deriveFieldCode(value);
  if (code === "ANDR") return "Ain Dar";
  if (code === "ABQQ") return "Abqaiq";
  return "Unknown";
}

function getStatusTone(status) {
  const s = String(status || "").toLowerCase();
  if (s.includes("high") || s.includes("shut") || s.includes("overdue")) return "destructive";
  if (s.includes("medium") || s.includes("testing") || s.includes("locked")) return "secondary";
  return "default";
}

export default function WellDetailsDrawer({ open, onClose, well }) {
  if (!open || !well) return null;

  return (
    <div className="fixed inset-0 z-50 flex justify-end bg-black/40">
      <button type="button" className="flex-1" onClick={onClose} aria-label="Close well details" />
      <div className="relative h-full w-full max-w-2xl border-l bg-background shadow-2xl">
        <div className="flex items-center justify-between border-b px-5 py-4">
          <div>
            <p className="text-sm text-muted-foreground">Well Drill-Down</p>
            <h2 className="text-xl font-semibold">{well.well_name}</h2>
          </div>
          <Button variant="ghost" size="icon" className="rounded-xl" onClick={onClose}>
            <X className="h-4 w-4" />
          </Button>
        </div>

        <ScrollArea className="h-[calc(100vh-73px)]">
          <div className="space-y-4 p-5">
            <div className="grid gap-3 md:grid-cols-2">
              <Card className="rounded-2xl">
                <CardHeader className="pb-3">
                  <CardTitle className="text-base">Well Context</CardTitle>
                </CardHeader>
                <CardContent className="space-y-3 text-sm">
                  <div className="flex items-center justify-between gap-3">
                    <span className="text-muted-foreground">Field</span>
                    <Badge variant="outline">{getFieldLabel(well.field_code || well.well_name)}</Badge>
                  </div>
                  <div className="flex items-center justify-between gap-3">
                    <span className="text-muted-foreground">Production Status</span>
                    <Badge variant={getStatusTone(well.production_status)}>{well.production_status || "—"}</Badge>
                  </div>
                  <div className="flex items-center justify-between gap-3">
                    <span className="text-muted-foreground">Oil Rate</span>
                    <span className="font-medium">{formatNumber(well.oil_rate_bopd)} BOPD</span>
                  </div>
                </CardContent>
              </Card>

              <Card className="rounded-2xl">
                <CardHeader className="pb-3">
                  <CardTitle className="text-base">Risk Profile</CardTitle>
                </CardHeader>
                <CardContent className="space-y-3 text-sm">
                  <div className="flex items-center justify-between gap-3">
                    <span className="text-muted-foreground">Risk Score</span>
                    <span className="font-medium">{formatNumber(well.risk_score)}</span>
                  </div>
                  <div className="flex items-center justify-between gap-3">
                    <span className="text-muted-foreground">Risk Level</span>
                    <Badge variant={getStatusTone(well.risk_level)}>{well.risk_level || "—"}</Badge>
                  </div>
                  <div className="flex items-center justify-between gap-3">
                    <span className="text-muted-foreground">Linked DNs</span>
                    <span className="font-medium">{formatNumber((well.linked_dns || []).length)}</span>
                  </div>
                </CardContent>
              </Card>
            </div>

            <Card className="rounded-2xl">
              <CardHeader className="pb-3">
                <CardTitle className="text-base">Operational Reasons</CardTitle>
              </CardHeader>
              <CardContent>
                {(well.reasons || []).length ? (
                  <div className="flex flex-wrap gap-2">
                    {(well.reasons || []).map((reason, index) => (
                      <Badge key={index} variant="outline" className="rounded-xl">{reason}</Badge>
                    ))}
                  </div>
                ) : (
                  <p className="text-sm text-muted-foreground">No risk reasons were returned for this well.</p>
                )}
              </CardContent>
            </Card>

            <Card className="rounded-2xl">
              <CardHeader className="pb-3">
                <CardTitle className="text-base">Linked DNs</CardTitle>
              </CardHeader>
              <CardContent>
                {(well.linked_dns || []).length ? (
                  <div className="space-y-3">
                    {(well.linked_dns || []).map((dn, index) => (
                      <div key={`${dn.dn_id}-${index}`} className="rounded-2xl border p-4">
                        <div className="flex flex-wrap items-center justify-between gap-3">
                          <div>
                            <div className="flex items-center gap-2">
                              <Wrench className="h-4 w-4" />
                              <p className="font-semibold">{dn.dn_id}</p>
                            </div>
                            <p className="mt-1 text-sm text-muted-foreground">
                              Owner: {dn.owner || "—"} • Priority: {dn.priority || "—"}
                            </p>
                          </div>
                          <div className="flex flex-wrap gap-2">
                            <Badge variant={getStatusTone(dn.priority)}>{dn.priority || "—"}</Badge>
                            {dn.overdue ? <Badge variant="destructive">Overdue</Badge> : null}
                            {dn.blocked ? <Badge variant="secondary">Blocked</Badge> : null}
                          </div>
                        </div>
                        <div className="mt-3 grid gap-3 sm:grid-cols-3">
                          <div className="rounded-xl bg-muted/50 p-3 text-sm">
                            <div className="flex items-center gap-2 text-muted-foreground">
                              <Gauge className="h-4 w-4" /> Progress
                            </div>
                            <p className="mt-1 font-medium">{formatNumber(dn.progress_percent)}%</p>
                          </div>
                          <div className="rounded-xl bg-muted/50 p-3 text-sm">
                            <div className="flex items-center gap-2 text-muted-foreground">
                              <AlertTriangle className="h-4 w-4" /> Severity
                            </div>
                            <p className="mt-1 font-medium">{String(dn.priority_severity || dn.priority || "—").toUpperCase()}</p>
                          </div>
                          <div className="rounded-xl bg-muted/50 p-3 text-sm">
                            <div className="flex items-center gap-2 text-muted-foreground">
                              <Wrench className="h-4 w-4" /> Well
                            </div>
                            <p className="mt-1 font-medium">{dn.well_name || "—"}</p>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="text-sm text-muted-foreground">No linked DN items were found for this well in the current UI state.</p>
                )}
              </CardContent>
            </Card>
          </div>
        </ScrollArea>
      </div>
    </div>
  );
}
