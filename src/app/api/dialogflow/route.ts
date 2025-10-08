import { NextRequest, NextResponse } from "next/server";

// ✅ Responds to verification GET requests
export async function GET() {
  return NextResponse.json({ status: "Webhook is live" }, { status: 200 });
}

// ✅ Responds safely to Dialogflow POST (even empty test body)
export async function POST(req: NextRequest) {
  try {
    const text = "Hello from Next.js webhook!";
    let body = {};
    try {
      body = await req.json();
      console.log("Dialogflow request:", body);
    } catch {
      console.log("No JSON body (verification POST)");
    }

    return NextResponse.json(
      {
        fulfillment_response: {
          messages: [{ text: { text: [text] } }],
        },
      },
      { status: 200 }
    );
  } catch (error) {
    console.error("Webhook error:", error);
    return NextResponse.json({ error: "Internal Server Error" }, { status: 500 });
  }
}
