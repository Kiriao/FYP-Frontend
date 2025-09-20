// pages/api/update-child-password.ts
import type { NextApiRequest, NextApiResponse } from "next";
import admin from "@/lib/firebaseAdmin"; // make sure Admin SDK is initialized

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  const { parentId, childId, newPassword } = req.body;

  if (!parentId || !childId || !newPassword) {
    return res.status(400).json({ success: false, error: "Missing parameters" });
  }

  try {
    // ✅ Update child password using Admin SDK
    await admin.auth().updateUser(childId, {
      password: newPassword,
    });

    res.status(200).json({ success: true });
  } catch (err: any) {
    console.error(err);
    res.status(400).json({ success: false, error: err.message });
  }
}
