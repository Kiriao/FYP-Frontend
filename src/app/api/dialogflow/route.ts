import { NextRequest, NextResponse } from "next/server";

export async function POST(req: NextRequest) {
  const body = await req.json();
  console.log("Dialogflow request:", body);

  // Handle intent logic here
  const fulfillmentText = "Hello from Next.js webhook!";

  return NextResponse.json({
    fulfillment_response: {
      messages: [
        {
          text: {
            text: [fulfillmentText],
          },
        },
      ],
    },
  });
}
