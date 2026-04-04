import React from "react";
import { X } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";

function formatNumber(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  return new Intl.NumberFormat().format(Number(value));
}

function getStatusTone(status) {
  const s = String(status || "").toLowerCase();
  if (s.includes("high") || s.includes("overdue") || s.includes("blocked")) return "destructive";
  if (s.includes("medium")) return "secondary";
  return "default";
}

export default function DNDetailsModal({ open, onClose, dn }) {
  if (!open || !dn) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
      <div className="w-full max-w-2xl rounded-3xl border bg-background shadow-2xl">
        <div className="flex items-center justify-between border-b px-5 py-4">
          <div>
            <p className="text-sm text-muted-foreground">DN Details</p>
            <h2 className="text-xl font-semibold">{dn.dn_id}</h2>
          </div>
          <Button variant="ghost" size="icon" className="rounded-xl" onClick={onClose}>
            <X className="h-4 w-4" />
          </Button>
        </div>

        <div className="space-y-4 p-5">
          <div className="grid gap-4 md:grid-cols-2">
            <Card className="rounded-2xl">
              <CardHeader className="pb-3">
                <CardTitle className="text-base">Core Context</CardTitle>
              </CardHeader>
              <CardContent className="space-y-3 text-sm">
                <div className="flex items-center justify-between gap-3">
                  <span className="text-muted-foreground">Well Name</span>
                  <span className="font-medium">{dn.well_name || "—"}</span>
                </div>
                <div className="flex items-center justify-between gap-3">
                  <span className="text-muted-foreground">Owner</span>
                  <span className="font-medium">{dn.owner || "—"}</span>
                </div>
                <div className="flex items-center justify-between gap-3">
                  <span className="text-muted-foreground">Priority</span>
                  <Badge variant={getStatusTone(dn.priority)}>{dn.priority || "—"}</Badge>
                </div>
              </CardContent>
            </Card>

            <Card className="rounded-2xl">
              <CardHeader className="pb-3">
                <CardTitle className="text-base">Derived Status</CardTitle>
              </CardHeader>
              <CardContent className="space-y-3 text-sm">
                <div className="flex items-center justify-between gap-3">
                  <span className="text-muted-foreground">Overdue</span>
                  <Badge variant={dn.overdue ? "destructive" : "outline"}>{dn.overdue ? "Yes" : "No"}</Badge>
                </div>
                <div className="flex items-center justify-between gap-3">
                  <span className="text-muted-foreground">Blocked</span>
                  <Badge variant={dn.blocked ? "destructive" : "outline"}>{dn.blocked ? "Yes" : "No"}</Badge>
                </div>
                <div className="flex items-center justify-between gap-3">
                  <span className="text-muted-foreground">Priority Severity</span>
                  <Badge variant={getStatusTone(dn.priority_severity)}>{dn.priority_severity || "—"}</Badge>
                </div>
              </CardContent>
            </Card>
          </div>

          <Card className="rounded-2xl">
            <CardHeader className="pb-3">
              <CardTitle className="text-base">Progress</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                <div className="flex items-center justify-between text-sm">
                  <span className="text-muted-foreground">Completion</span>
                  <span className="font-medium">{formatNumber(dn.progress_percent)}%</span>
                </div>
                <div className="h-3 overflow-hidden rounded-full bg-muted">
                  <div
                    className="h-full rounded-full bg-foreground transition-all"
                    style={{ width: `${Math.max(0, Math.min(100, Number(dn.progress_percent || 0)))}%` }}
                  />
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
