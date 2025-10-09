import { NextResponse } from "next/server";

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const query = searchParams.get("q") || "harry potter";
  const offset = searchParams.get("offset") || "0";
  const limit = searchParams.get("limit") || "20";

  // Add Offset and Limit to the NLB API URL
  const apiUrl = `https://openweb.nlb.gov.sg/api/v2/Catalogue/SearchTitles?Keywords=${encodeURIComponent(
    query
  )}&Offset=${offset}&Limit=${limit}`;

  console.log('NLB API URL:', apiUrl); // Debug log

  const res = await fetch(apiUrl, {
    headers: {
      "X-APP-ID": process.env.NLB_APP_ID || "",
      "X-APP-CODE": process.env.NLB_APP_CODE || "",
      "X-API-KEY": process.env.NLB_API_KEY || "",
    },
  });

  const text = await res.text();
  let data: any;
  try {
    data = JSON.parse(text);
  } catch {
    data = { raw: text };
  }

  if (!res.ok) {
    return NextResponse.json(
      { error: `NLB API error: ${res.statusText}`, status: res.status, body: data },
      { status: res.status }
    );
  }

  return NextResponse.json(data);
}