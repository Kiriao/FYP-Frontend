'use client';

import { useSearchParams, useRouter } from "next/navigation";
import { useState, useEffect } from "react";
import { getAuth } from "firebase/auth";
import { doc, getDoc, updateDoc, addDoc, collection, serverTimestamp } from "firebase/firestore";
import { db } from "@/lib/firebase";

export default function CheckoutClient() {
  const searchParams = useSearchParams();
  const router = useRouter();
  const planId = searchParams?.get("plan") ?? "";
  const [processing, setProcessing] = useState(false);
  const [planName, setPlanName] = useState("Loading...");
  const [price, setPrice] = useState<string>("0");
  const [loading, setLoading] = useState(true);
  const [isTrial, setIsTrial] = useState(false); // ✅ Track if it's a trial plan
  const [trialDays, setTrialDays] = useState(7); // ✅ Trial duration

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
          setPrice(data.price || "0");
          setIsTrial(data.isTrial || false); // ✅ Check if it's a trial
          setTrialDays(data.trialDays || 7); // ✅ Get trial duration
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

      // ✅ Calculate expiration date based on plan type
      const currentDate = new Date();
      let expirationDate: Date;
      
      if (isTrial) {
        // Trial plan: use trial duration
        expirationDate = new Date(currentDate.getTime() + (trialDays * 24 * 60 * 60 * 1000));
      } else {
        // Regular plan: 30 days
        expirationDate = new Date(currentDate.getTime() + (30 * 24 * 60 * 60 * 1000));
      }

      // 1️⃣ Update user's active plan with expiration date
      const userRef = doc(db, "users", user.uid);
      const updateData: any = {
        plan: planName,
        planUpdatedAt: serverTimestamp(),
        planExpiresAt: expirationDate,
        isActive: true,
      };

      // ✅ If it's a trial, mark trial as used
      if (isTrial) {
        updateData.hasUsedTrial = true;
        updateData.trialUsedAt = serverTimestamp();
      }

      await updateDoc(userRef, updateData);

      // 2️⃣ Add subscription record with expiration
      const subscriptionData: any = {
        userId: user.uid,
        plan: planName,
        amount: isTrial ? "0" : price, // ✅ Trial is free
        status: isTrial ? "trial" : "paid", // ✅ Different status for trial
        createdAt: serverTimestamp(),
        expiresAt: expirationDate,
        isActive: true,
      };

      // ✅ Add trial-specific fields
      if (isTrial) {
        subscriptionData.isTrial = true;
        subscriptionData.trialDays = trialDays;
      }

      await addDoc(collection(db, "subscriptions"), subscriptionData);

      // 3️⃣ Redirect appropriately
      if (isTrial) {
        // For trial, go directly back to plans page with success message
        alert(`🎉 Your ${trialDays}-day free trial has started! Enjoy ${planName}!`);
        router.push("/plans"); // or wherever your plans page is
      } else {
        // For paid plans, go to payment page
        router.push(`/payment?plan=${encodeURIComponent(planName)}`);
      }
      
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

  // ✅ If it's a trial plan, show trial-specific UI
  if (isTrial) {
    return (
      <section className="bg-white py-20 px-6">
        <div className="max-w-3xl mx-auto text-center p-10 rounded-2xl shadow-lg border border-purple-200 bg-gradient-to-br from-purple-50 to-pink-50">
          <div className="mb-6">
            <span className="bg-purple-600 text-white px-4 py-2 rounded-full text-sm font-semibold">
              FREE TRIAL
            </span>
          </div>
          
          <h1 className="text-3xl font-bold text-purple-600 mb-6">Start Your Free Trial</h1>
          <p className="text-lg text-gray-700 mb-4">
            You selected: <span className="font-semibold text-purple-600">{planName}</span>
          </p>
          <p className="text-gray-600 mb-4">
            Duration: <span className="font-semibold text-purple-600">{trialDays} days FREE</span>
          </p>
          <p className="text-gray-600 mb-8">
            Start your free trial now - no payment required! You can upgrade to a paid plan anytime.
          </p>

          <div className="bg-purple-100 border border-purple-300 rounded-lg p-4 mb-8">
            <h3 className="font-semibold text-purple-800 mb-2">Trial Benefits:</h3>
            <ul className="text-purple-700 text-sm space-y-1">
              <li>✅ Full access to all premium features</li>
              <li>✅ No credit card required</li>
              <li>✅ Cancel anytime</li>
              <li>✅ Automatic conversion to free plan after {trialDays} days</li>
            </ul>
          </div>

          <button
            onClick={handlePaymentRedirect}
            disabled={processing}
            className={`px-6 py-3 rounded-lg font-bold shadow-lg ${
              processing
                ? "bg-gray-400 text-white cursor-not-allowed"
                : "bg-purple-600 hover:bg-purple-700 text-white"
            }`}
          >
            {processing ? "Starting Trial..." : `Start ${trialDays}-Day Free Trial`}
          </button>
          
          <p className="text-xs text-gray-500 mt-4">
            * Trial can only be used once per account
          </p>
        </div>
      </section>
    );
  }

  // ✅ Regular paid plan UI
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