// src/app/api/moderate-proxy/route.ts
import { NextRequest, NextResponse } from "next/server";

// Use your asia-southeast1 URL
const MOD_URL = process.env.NEXT_PUBLIC_MODERATOR_URL!; // e.g. https://moderatemessage-xxxxx-as.a.run.app

export async function POST(req: NextRequest) {
  try {
    const body = await req.json();
    const r = await fetch(MOD_URL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    const j = await r.json();
    return NextResponse.json(j, { status: r.status });
  } catch (e: any) {
    return NextResponse.json({ blocked: false, error: e?.message ?? "proxy error" }, { status: 500 });
  }
}
