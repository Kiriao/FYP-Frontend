// app/api/github/remove/route.ts
import { NextResponse } from "next/server";

const GITHUB_TOKEN = process.env.GITHUB_TOKEN!;
const REPO = "Kiriao/Rest-API";
const BRANCH = "main";

export async function POST(req: Request) {
  try {
    const { category, filename, sha } = await req.json();

    const url = `https://api.github.com/repos/${REPO}/contents/content/${category}/${filename}`;

    const res = await fetch(url, {
      method: "DELETE",
      headers: {
        Authorization: `Bearer ${GITHUB_TOKEN}`,
        Accept: "application/vnd.github.v3+json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        message: `Remove ${filename}`,
        branch: BRANCH,
        sha, // 👈 required
      }),
    });

    const data = await res.json();
    if (!res.ok) throw new Error(data.message);
    return NextResponse.json(data);
  } catch (err: any) {
    return NextResponse.json({ error: err.message }, { status: 500 });
  }
}
