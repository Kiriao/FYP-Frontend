'use client';

import { useSearchParams, useRouter } from "next/navigation";
import { useState, useEffect, useCallback } from "react";
import { getAuth } from "firebase/auth";
import { doc, getDoc, updateDoc, addDoc, collection, serverTimestamp } from "firebase/firestore";
import { db } from "@/lib/firebase";

interface PlanData {
  name: string;
  price: number;
  isTrial?: boolean;
  trialDays?: number;
  features?: string[];
  description?: string;
  isFree?: boolean;
}

interface LoadingState {
  isLoading: boolean;
  message: string;
}

export default function CheckoutClient() {
  const searchParams = useSearchParams();
  const router = useRouter();
  const planId = searchParams?.get("plan") ?? "";
  
  const [processing, setProcessing] = useState(false);
  const [planData, setPlanData] = useState<PlanData | null>(null);
  const [loadingState, setLoadingState] = useState<LoadingState>({
    isLoading: true,
    message: "Loading plan details..."
  });
  const [error, setError] = useState<string | null>(null);

  // Fetch plan info from Firestore
  const fetchPlan = useCallback(async () => {
    if (!planId) {
      setError("No plan selected. Please go back and select a plan.");
      setLoadingState({ isLoading: false, message: "" });
      return;
    }

    try {
      setLoadingState({ isLoading: true, message: "Loading plan details..." });
      
      const planRef = doc(db, "plans", planId);
      const planSnap = await getDoc(planRef);

      if (planSnap.exists()) {
        const data = planSnap.data() as PlanData;
        
        // Validate plan data
        if (!data.name) {
          throw new Error("Invalid plan data");
        }

        // Don't allow free plans in checkout
        if (data.isFree) {
          setError("Free plans don't require checkout. Redirecting...");
          setTimeout(() => router.push("/plans"), 2000);
          return;
        }

        setPlanData(data);
        setError(null);
      } else {
        setError("Plan not found. Please go back and select a valid plan.");
      }
    } catch (err: any) {
      console.error("Error fetching plan:", err);
      setError(err.message || "Failed to load plan details. Please try again.");
    } finally {
      setLoadingState({ isLoading: false, message: "" });
    }
  }, [planId, router]);

  useEffect(() => {
    fetchPlan();
  }, [fetchPlan]);

  const calculateExpirationDate = (isTrial: boolean, trialDays: number): Date => {
    const currentDate = new Date();
    const durationDays = isTrial ? trialDays : 30;
    return new Date(currentDate.getTime() + (durationDays * 24 * 60 * 60 * 1000));
  };

  const handleSubscriptionActivation = async () => {
    if (!planData) return;

    setProcessing(true);
    setError(null);

    try {
      const auth = getAuth();
      const user = auth.currentUser;

      if (!user) {
        throw new Error("You must be logged in to subscribe. Please log in and try again.");
      }

      const userRef = doc(db, "users", user.uid);
      const userSnap = await getDoc(userRef);

      if (!userSnap.exists()) {
        throw new Error("User profile not found. Please complete your registration.");
      }

      const userData = userSnap.data();

      // Check if user has already used trial
      if (planData.isTrial && userData.hasUsedTrial) {
        throw new Error("You have already used your free trial. Please select a paid plan.");
      }

      // Calculate expiration date
      const expirationDate = calculateExpirationDate(
        planData.isTrial || false, 
        planData.trialDays || 7
      );

      // Prepare update data
      const updateData: any = {
        plan: planData.name,
        planUpdatedAt: serverTimestamp(),
        planExpiresAt: expirationDate,
        isActive: true,
      };

      if (planData.isTrial) {
        updateData.hasUsedTrial = true;
        updateData.trialUsedAt = serverTimestamp();
      }

      // Update user's plan
      await updateDoc(userRef, updateData);

      // Create subscription record
      const subscriptionData: any = {
        userId: user.uid,
        userEmail: user.email,
        plan: planData.name,
        planId: planId,
        amount: planData.isTrial ? 0 : planData.price,
        status: planData.isTrial ? "trial" : "paid",
        createdAt: serverTimestamp(),
        expiresAt: expirationDate,
        isActive: true,
      };

      if (planData.isTrial) {
        subscriptionData.isTrial = true;
        subscriptionData.trialDays = planData.trialDays || 7;
      }

      await addDoc(collection(db, "subscriptions"), subscriptionData);

      // Redirect based on plan type
      if (planData.isTrial) {
        setLoadingState({ 
          isLoading: true, 
          message: `Activating your ${planData.trialDays || 7}-day free trial...` 
        });
        
        setTimeout(() => {
          alert(`🎉 Success! Your ${planData.trialDays || 7}-day free trial has started. Enjoy ${planData.name}!`);
          router.push("/plans");
        }, 1500);
      } else {
        setLoadingState({ 
          isLoading: true, 
          message: "Redirecting to payment..." 
        });
        
        router.push(`/payment?plan=${encodeURIComponent(planData.name)}&planId=${encodeURIComponent(planId)}`);
      }
      
    } catch (error: any) {
      console.error("Error processing subscription:", error);
      setError(error.message || "Failed to process your subscription. Please try again.");
      setProcessing(false);
    }
  };

  const handleGoBack = () => {
    router.push("/plans");
  };

  // Loading state
  if (loadingState.isLoading) {
    return (
      <main className="flex items-center justify-center min-h-screen bg-gradient-to-b from-pink-50 to-white">
        <div className="text-center">
          <div className="animate-spin rounded-full h-16 w-16 border-b-4 border-pink-600 mx-auto mb-4"></div>
          <p className="text-gray-600 text-lg">{loadingState.message}</p>
        </div>
      </main>
    );
  }

  // Error state
  if (error) {
    return (
      <main className="flex items-center justify-center min-h-screen bg-gradient-to-b from-pink-50 to-white px-6">
        <div className="max-w-md mx-auto text-center p-8 rounded-2xl shadow-lg bg-white border-2 border-red-200">
          <div className="w-16 h-16 bg-red-100 rounded-full flex items-center justify-center mx-auto mb-4">
            <span className="text-3xl">⚠️</span>
          </div>
          <h2 className="text-2xl font-bold text-red-600 mb-4">Oops!</h2>
          <p className="text-gray-700 mb-6">{error}</p>
          <button
            onClick={handleGoBack}
            className="px-6 py-3 bg-pink-600 hover:bg-pink-700 text-white font-semibold rounded-lg shadow-md transition-all duration-300"
          >
            Back to Plans
          </button>
        </div>
      </main>
    );
  }

  // No plan data
  if (!planData) {
    return (
      <main className="flex items-center justify-center min-h-screen bg-gradient-to-b from-pink-50 to-white px-6">
        <div className="max-w-md mx-auto text-center p-8 rounded-2xl shadow-lg bg-white">
          <p className="text-gray-700 mb-6">No plan information available.</p>
          <button
            onClick={handleGoBack}
            className="px-6 py-3 bg-pink-600 hover:bg-pink-700 text-white font-semibold rounded-lg shadow-md transition-all duration-300"
          >
            Back to Plans
          </button>
        </div>
      </main>
    );
  }

  // Trial plan UI
  if (planData.isTrial) {
    const trialDays = planData.trialDays || 7;

    return (
      <section className="bg-gradient-to-b from-purple-50 to-white min-h-screen py-20 px-6">
        <div className="max-w-4xl mx-auto">
          {/* Back Button */}
          <button
            onClick={handleGoBack}
            className="mb-6 text-purple-600 hover:text-purple-700 font-semibold flex items-center transition-colors"
          >
            ← Back to Plans
          </button>

          <div className="bg-white p-10 rounded-3xl shadow-2xl border-2 border-purple-200">
            {/* Trial Badge */}
            <div className="flex justify-center mb-6">
              <span className="bg-gradient-to-r from-purple-600 to-pink-600 text-white px-6 py-2 rounded-full text-sm font-bold shadow-lg">
                🎉 FREE TRIAL
              </span>
            </div>
            
            <h1 className="text-4xl font-bold text-purple-600 mb-4 text-center">
              Start Your Free Trial
            </h1>
            
            <p className="text-center text-gray-600 mb-8">
              No credit card required • Cancel anytime
            </p>

            {/* Plan Details */}
            <div className="bg-gradient-to-br from-purple-50 to-pink-50 rounded-2xl p-8 mb-8">
              <div className="text-center mb-6">
                <h2 className="text-3xl font-bold text-purple-700 mb-2">
                  {planData.name}
                </h2>
                {planData.description && (
                  <p className="text-gray-600">{planData.description}</p>
                )}
              </div>

              <div className="flex justify-center items-baseline mb-6">
                <span className="text-5xl font-bold text-purple-600">FREE</span>
                <span className="text-gray-600 ml-3">for {trialDays} days</span>
              </div>

              {planData.features && planData.features.length > 0 && (
                <div className="mt-6">
                  <h3 className="font-bold text-purple-700 mb-3 text-center">
                    What's Included:
                  </h3>
                  <ul className="space-y-2">
                    {planData.features.map((feature, idx) => (
                      <li key={idx} className="flex items-start">
                        <span className="text-purple-600 font-bold mr-2 text-lg">✓</span>
                        <span className="text-gray-700">{feature}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>

            {/* Trial Benefits */}
            <div className="bg-purple-100 border-2 border-purple-300 rounded-xl p-6 mb-8">
              <h3 className="font-bold text-purple-800 mb-3 text-center">
                Trial Benefits:
              </h3>
              <ul className="text-purple-700 space-y-2">
                <li className="flex items-center">
                  <span className="mr-2">✅</span>
                  <span>Full access to all premium features</span>
                </li>
                <li className="flex items-center">
                  <span className="mr-2">✅</span>
                  <span>No payment information required</span>
                </li>
                <li className="flex items-center">
                  <span className="mr-2">✅</span>
                  <span>Cancel anytime with no obligations</span>
                </li>
                <li className="flex items-center">
                  <span className="mr-2">✅</span>
                  <span>Automatically reverts to Free plan after {trialDays} days</span>
                </li>
              </ul>
            </div>

            {/* Action Button */}
            <button
              onClick={handleSubscriptionActivation}
              disabled={processing}
              className={`w-full py-4 rounded-xl font-bold text-lg shadow-lg transition-all duration-300 ${
                processing
                  ? "bg-gray-400 text-white cursor-not-allowed"
                  : "bg-gradient-to-r from-purple-600 to-pink-600 hover:from-purple-700 hover:to-pink-700 text-white transform hover:scale-105"
              }`}
            >
              {processing ? (
                <span className="flex items-center justify-center">
                  <span className="animate-spin mr-2">⏳</span>
                  Starting Trial...
                </span>
              ) : (
                `Start My ${trialDays}-Day Free Trial`
              )}
            </button>
            
            <p className="text-center text-xs text-gray-500 mt-4">
              * Free trial can only be used once per account
            </p>
          </div>
        </div>
      </section>
    );
  }

  // Regular paid plan UI
  return (
    <section className="bg-gradient-to-b from-pink-50 to-white min-h-screen py-20 px-6">
      <div className="max-w-4xl mx-auto">
        {/* Back Button */}
        <button
          onClick={handleGoBack}
          className="mb-6 text-pink-600 hover:text-pink-700 font-semibold flex items-center transition-colors"
        >
          ← Back to Plans
        </button>

        <div className="bg-white p-10 rounded-3xl shadow-2xl border-2 border-gray-200">
          <div className="text-center mb-8">
            <h1 className="text-4xl font-bold text-pink-600 mb-2">Checkout</h1>
            <p className="text-gray-600">Review your subscription details</p>
          </div>

          {/* Plan Details */}
          <div className="bg-gradient-to-br from-pink-50 to-white rounded-2xl p-8 mb-8">
            <div className="text-center mb-6">
              <h2 className="text-3xl font-bold text-pink-700 mb-2">
                {planData.name}
              </h2>
              {planData.description && (
                <p className="text-gray-600">{planData.description}</p>
              )}
            </div>

            <div className="flex justify-center items-baseline mb-6">
              <span className="text-5xl font-bold text-pink-600">
                ${planData.price.toFixed(2)}
              </span>
              <span className="text-gray-600 ml-3">/ month</span>
            </div>

            <div className="text-center mb-6">
              <div className="inline-block bg-green-100 border border-green-300 rounded-lg px-4 py-2">
                <p className="text-green-700 font-semibold">
                  ✓ Valid for 30 days from purchase
                </p>
              </div>
            </div>

            {planData.features && planData.features.length > 0 && (
              <div className="mt-6">
                <h3 className="font-bold text-pink-700 mb-3 text-center">
                  What's Included:
                </h3>
                <ul className="space-y-2 max-w-md mx-auto">
                  {planData.features.map((feature, idx) => (
                    <li key={idx} className="flex items-start">
                      <span className="text-pink-600 font-bold mr-2 text-lg">✓</span>
                      <span className="text-gray-700">{feature}</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>

          {/* Security Notice */}
          <div className="bg-blue-50 border border-blue-200 rounded-xl p-4 mb-8">
            <div className="flex items-start">
              <span className="text-2xl mr-3">🔒</span>
              <div>
                <h4 className="font-semibold text-blue-900 mb-1">Secure Payment</h4>
                <p className="text-sm text-blue-700">
                  Your payment information is encrypted and secure. We use industry-standard security measures to protect your data.
                </p>
              </div>
            </div>
          </div>

          {/* Action Buttons */}
          <div className="space-y-3">
            <button
              onClick={handleSubscriptionActivation}
              disabled={processing}
              className={`w-full py-4 rounded-xl font-bold text-lg shadow-lg transition-all duration-300 ${
                processing
                  ? "bg-gray-400 text-white cursor-not-allowed"
                  : "bg-gradient-to-r from-green-600 to-green-700 hover:from-green-700 hover:to-green-800 text-white transform hover:scale-105"
              }`}
            >
              {processing ? (
                <span className="flex items-center justify-center">
                  <span className="animate-spin mr-2">⏳</span>
                  Processing...
                </span>
              ) : (
                "Proceed to Payment →"
              )}
            </button>

            <button
              onClick={handleGoBack}
              disabled={processing}
              className="w-full py-3 rounded-xl font-semibold text-gray-600 hover:text-gray-800 hover:bg-gray-100 transition-all duration-300 border-2 border-gray-300"
            >
              Cancel
            </button>
          </div>

          <p className="text-center text-xs text-gray-500 mt-6">
            By proceeding, you agree to our Terms of Service and Privacy Policy
          </p>
        </div>
      </div>
    </section>
  );
}