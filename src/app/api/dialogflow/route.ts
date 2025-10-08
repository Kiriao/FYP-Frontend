import { NextRequest, NextResponse } from "next/server";

// ✅ Handle Dialogflow’s initial verification GET
export async function GET() {
  return NextResponse.json({ status: "Webhook is live" }, { status: 200 });
}

// ✅ Handle POST requests from Dialogflow
export async function POST(req: NextRequest) {
  try {
    const body = await req.json();
    console.log("Dialogflow request:", body);

    // Your fulfillment logic here
    const fulfillmentText = "Hello from Next.js webhook!";

    return NextResponse.json({
      fulfillment_response: {
        messages: [
          {
            text: { text: [fulfillmentText] },
          },
        ],
      },
    });
  } catch (error) {
    console.error("Webhook error:", error);
    return NextResponse.json({ error: "Invalid request" }, { status: 400 });
  }
}
