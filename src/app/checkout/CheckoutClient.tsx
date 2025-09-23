'use client';

import { useSearchParams, useRouter } from "next/navigation";
import { useState, useEffect } from "react";
import { getAuth } from "firebase/auth";
import { doc, getDoc, updateDoc, addDoc, collection, serverTimestamp } from "firebase/firestore";
import { db } from "@/lib/firebase";

export default function CheckoutClient() {
  const searchParams = useSearchParams();
  const router = useRouter();
  const planId = searchParams?.get("plan") ?? ""; // planId should match the Firestore doc id
  const [processing, setProcessing] = useState(false);
  const [planName, setPlanName] = useState("Loading...");
  const [price, setPrice] = useState<string>("0");
  const [loading, setLoading] = useState(true);

  // 🔹 Fetch plan info dynamically from Firestore
  useEffect(() => {
    const fetchPlan = async () => {
      try {
        if (!planId) {
          setPlanName("Unknown Plan");
          setPrice("0");
          setLoading(false);
          return;
        }

        const planRef = doc(db, "plans", planId);
        const planSnap = await getDoc(planRef);

        if (planSnap.exists()) {
          const data = planSnap.data();
          setPlanName(data.name || "Unknown Plan");
          setPrice(data.price || "0"); // price is stored as string in your DB
        } else {
          setPlanName("Unknown Plan");
          setPrice("0");
        }
      } catch (err) {
        console.error("Error fetching plan:", err);
        setPlanName("Unknown Plan");
        setPrice("0");
      } finally {
        setLoading(false);
      }
    };

    fetchPlan();
  }, [planId]);

  const handlePaymentRedirect = async () => {
    setProcessing(true);

    try {
      const auth = getAuth();
      const user = auth.currentUser;

      if (!user) {
        alert("You must be logged in to subscribe.");
        setProcessing(false);
        return;
      }

      // 🔹 Calculate expiration date (30 days from now)
      const currentDate = new Date();
      const expirationDate = new Date(currentDate.getTime() + (30 * 24 * 60 * 60 * 1000)); // 30 days in milliseconds

      // 1️⃣ Update user's active plan with expiration date
      const userRef = doc(db, "users", user.uid);
      await updateDoc(userRef, {
        plan: planName,
        planUpdatedAt: serverTimestamp(),
        planExpiresAt: expirationDate, // ✅ Add expiration date
        isActive: true, // ✅ Mark as active
      });

      // 2️⃣ Add subscription record (history) with expiration
      await addDoc(collection(db, "subscriptions"), {
        userId: user.uid,
        plan: planName,
        amount: price, // string value from Firestore
        status: "paid",
        createdAt: serverTimestamp(),
        expiresAt: expirationDate, // ✅ Add expiration date to subscription record
        isActive: true, // ✅ Mark as active
      });

      // 3️⃣ Redirect to confirmation page
      router.push(`/payment?plan=${encodeURIComponent(planName)}`);
    } catch (error: any) {
      console.error("Error processing subscription:", error.message || error);
      alert(`Failed to process your subscription: ${error.message || error}`);
      setProcessing(false);
    }
  };

  if (loading) {
    return (
      <main className="flex items-center justify-center h-screen">
        <p className="text-gray-600">Loading plan details...</p>
      </main>
    );
  }

  return (
    <section className="bg-white py-20 px-6">
      <div className="max-w-3xl mx-auto text-center p-10 rounded-2xl shadow-lg border border-gray-200">
        <h1 className="text-3xl font-bold text-pink-600 mb-6">Checkout</h1>
        <p className="text-lg text-gray-700 mb-4">
          You selected: <span className="font-semibold text-pink-600">{planName}</span>
        </p>
        <p className="text-gray-600 mb-4">
          Price: <span className="font-semibold text-pink-600">${price}</span>
        </p>
        <p className="text-gray-600 mb-8">
          <span className="font-semibold">Valid for 30 days</span> from the date of purchase.
        </p>
        <p className="text-gray-600 mb-8">Confirm your subscription to continue.</p>

        <button
          onClick={handlePaymentRedirect}
          disabled={processing}
          className={`px-6 py-3 rounded-lg font-bold shadow-lg ${
            processing
              ? "bg-gray-400 text-white cursor-not-allowed"
              : "bg-green-600 hover:bg-green-700 text-white"
          }`}
        >
          {processing ? "Processing..." : `Proceed to Payment`}
        </button>
      </div>
    </section>
  );
}