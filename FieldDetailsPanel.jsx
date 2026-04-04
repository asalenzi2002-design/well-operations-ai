import React from "react";
import { X } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";

function formatNumber(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  return new Intl.NumberFormat().format(Number(value));
}

export default function FieldDetailsPanel({ open, onClose, field }) {
  if (!open || !field) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-end justify-center bg-black/40 p-4 md:items-center">
      <div className="w-full max-w-3xl rounded-3xl border bg-background shadow-2xl">
        <div className="flex items-center justify-between border-b px-5 py-4">
          <div>
            <p className="text-sm text-muted-foreground">Field Drill-Down</p>
            <h2 className="text-xl font-semibold">{field.field_name || field.name || field.field_code}</h2>
          </div>
          <Button variant="ghost" size="icon" className="rounded-xl" onClick={onClose}>
            <X className="h-4 w-4" />
          </Button>
        </div>

        <div className="space-y-4 p-5">
          <div className="grid gap-4 md:grid-cols-4">
            <Card className="rounded-2xl md:col-span-2">
              <CardHeader className="pb-3">
                <CardTitle className="text-base">Total Production</CardTitle>
              </CardHeader>
              <CardContent>
                <p className="text-3xl font-bold">{formatNumber(field.total_production || field.value)} BOPD</p>
              </CardContent>
            </Card>
            <Card className="rounded-2xl">
              <CardHeader className="pb-3">
                <CardTitle className="text-base">Well Count</CardTitle>
              </CardHeader>
              <CardContent>
                <p className="text-3xl font-bold">{formatNumber(field.well_count)}</p>
              </CardContent>
            </Card>
            <Card className="rounded-2xl">
              <CardHeader className="pb-3">
                <CardTitle className="text-base">DN Pressure</CardTitle>
              </CardHeader>
              <CardContent>
                <p className="text-3xl font-bold">{formatNumber(field.dn_pressure)}</p>
              </CardContent>
            </Card>
          </div>

          <Card className="rounded-2xl">
            <CardHeader className="pb-3">
              <CardTitle className="text-base">High-Risk Wells</CardTitle>
            </CardHeader>
            <CardContent>
              {(field.high_risk_wells || []).length ? (
                <div className="flex flex-wrap gap-2">
                  {(field.high_risk_wells || []).map((well, index) => (
                    <Badge key={`${well.well_name}-${index}`} variant="outline" className="rounded-xl">
                      {well.well_name} • {well.risk_level}
                    </Badge>
                  ))}
                </div>
              ) : (
                <p className="text-sm text-muted-foreground">No visible high-risk wells are attached to this field in the current dataset.</p>
              )}
            </CardContent>
          </Card>

          <Card className="rounded-2xl">
            <CardHeader className="pb-3">
              <CardTitle className="text-base">Field Insight</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm leading-6 text-muted-foreground">{field.insight || "No insight was generated for this field."}</p>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
