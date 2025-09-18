// app/api/github/update/route.ts
import { NextResponse } from "next/server";

const GITHUB_TOKEN = process.env.GITHUB_TOKEN!;
const REPO = "Kiriao/Rest-API";
const BRANCH = "main";

export async function POST(req: Request) {
  try {
    const { category, filename, content, sha } = await req.json();

    const encoded = Buffer.from(
      JSON.stringify(content, null, 2)
    ).toString("base64");

    const url = `https://api.github.com/repos/${REPO}/contents/content/${category}/${filename}`;

    const res = await fetch(url, {
      method: "PUT",
      headers: {
        Authorization: `Bearer ${GITHUB_TOKEN}`,
        Accept: "application/vnd.github.v3+json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        message: `Update ${filename}`,
        branch: BRANCH,
        content: encoded,
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
