'use client';

import { useState, useEffect, useCallback } from "react";
import { useRouter } from "next/navigation";
import { auth, db } from "@/lib/firebase";
import { onAuthStateChanged, User } from "firebase/auth";
import {
  doc,
  getDoc,
  setDoc,
  updateDoc,
  collection,
  getDocs,
  query,
  orderBy,
} from "firebase/firestore";

interface Plan {
  id: string;
  name: string;
  price: number;
  features: string[];
  isFree?: boolean;
  isTrial?: boolean;
  trialDays?: number;
  recommended?: boolean;
  description?: string;
}

interface UserPlanInfo {
  planName: string;
  expiresAt?: any;
  isActive?: boolean;
  remainingDays?: number;
  hasUsedTrial?: boolean;
  trialUsedAt?: any;
}

export default function SubscriptionPage() {
  const router = useRouter();
  const [user, setUser] = useState<User | null>(null);
  const [isRegistered, setIsRegistered] = useState(false);
  const [loading, setLoading] = useState(true);
  const [plans, setPlans] = useState<Plan[]>([]);
  const [selectedPlan, setSelectedPlan] = useState<Plan | null>(null);
  const [userPlanInfo, setUserPlanInfo] = useState<UserPlanInfo | null>(null);
  const [isProcessing, setIsProcessing] = useState(false);

  // Helper functions for expiry checking
  const isPlanExpired = useCallback((expirationDate: any): boolean => {
    if (!expirationDate) return true;
    const expDate = expirationDate.toDate ? expirationDate.toDate() : new Date(expirationDate);
    return new Date() > expDate;
  }, []);

  const getRemainingDays = useCallback((expirationDate: any): number => {
    if (!expirationDate) return 0;
    const expDate = expirationDate.toDate ? expirationDate.toDate() : new Date(expirationDate);
    const diffTime = expDate.getTime() - new Date().getTime();
    return Math.max(0, Math.ceil(diffTime / (1000 * 60 * 60 * 24)));
  }, []);

  // Handle expired plans
  const handleExpiredPlan = useCallback(async (userRef: any) => {
    console.log("Plan has expired, downgrading to Free Plan");
    
    try {
      await updateDoc(userRef, {
        plan: "Free Plan",
        isActive: false,
        planExpiredAt: new Date(),
      });
      return "Free Plan";
    } catch (error) {
      console.error("Error handling expired plan:", error);
      return "Free Plan";
    }
  }, []);

  // Fetch plans from Firestore
  const fetchPlans = useCallback(async () => {
    try {
      const q = query(collection(db, "plans"), orderBy("price", "asc"));
      const snapshot = await getDocs(q);

      const plansList: Plan[] = snapshot.docs.map((docSnap) => ({
        id: docSnap.id,
        ...(docSnap.data() as Omit<Plan, "id">),
      }));

      setPlans(plansList);
      return plansList;
    } catch (error) {
      console.error("Error fetching plans:", error);
      return [];
    }
  }, []);

  // Initialize user data and plans
  const initializeUserData = useCallback(async (currentUser: User) => {
    try {
      const userRef = doc(db, "users", currentUser.uid);
      const userSnap = await getDoc(userRef);

      if (!userSnap.exists()) {
        setIsRegistered(false);
        return;
      }

      setIsRegistered(true);
      const userData = userSnap.data();
      let currentPlanName = "Free Plan";

      // Check if user has a plan and if it's expired
      if (userData.plan && userData.plan !== "Free Plan") {
        const planExpiresAt = userData.planExpiresAt;
        const isActive = userData.isActive;

        if (planExpiresAt && !isPlanExpired(planExpiresAt) && isActive) {
          // Plan is still valid
          currentPlanName = userData.plan;
          const remainingDays = getRemainingDays(planExpiresAt);
          
          setUserPlanInfo({
            planName: currentPlanName,
            expiresAt: planExpiresAt,
            isActive: true,
            remainingDays,
            hasUsedTrial: userData.hasUsedTrial || false,
            trialUsedAt: userData.trialUsedAt,
          });
        } else {
          // Plan has expired or is inactive
          currentPlanName = await handleExpiredPlan(userRef);
          setUserPlanInfo({
            planName: currentPlanName,
            isActive: false,
            hasUsedTrial: userData.hasUsedTrial || false,
            trialUsedAt: userData.trialUsedAt,
          });
        }
      } else {
        // No plan or already on Free Plan
        if (!userData.plan) {
          await updateDoc(userRef, { plan: "Free Plan" }).catch(async () => {
            await setDoc(userRef, { plan: "Free Plan" }, { merge: true });
          });
        }
        
        setUserPlanInfo({
          planName: "Free Plan",
          isActive: true,
          hasUsedTrial: userData.hasUsedTrial || false,
          trialUsedAt: userData.trialUsedAt,
        });
      }

      // Fetch and set plans
      const fetchedPlans = await fetchPlans();
      const validPlan = fetchedPlans.find((p) => p.name === currentPlanName);
      setSelectedPlan(validPlan || null);
    } catch (error) {
      console.error("Error initializing user data:", error);
    }
  }, [isPlanExpired, getRemainingDays, handleExpiredPlan, fetchPlans]);

  useEffect(() => {
    const unsubscribe = onAuthStateChanged(auth, async (currentUser) => {
      setUser(currentUser);

      if (currentUser) {
        await initializeUserData(currentUser);
      } else {
        setIsRegistered(false);
        await fetchPlans();
      }

      setLoading(false);
    });

    return () => unsubscribe();
  }, [initializeUserData, fetchPlans]);

  const handleSelectPlan = async (plan: Plan) => {
    if (!user || !isRegistered) {
      alert("You must be a registered user to select a plan.");
      return;
    }

    if (isProcessing) return;

    // Check if it's a trial plan and user has already used trial
    if (plan.isTrial && userPlanInfo?.hasUsedTrial) {
      alert("You have already used your free trial. Please select a paid plan.");
      return;
    }

    // Prevent selecting the same plan
    if (selectedPlan?.id === plan.id && plan.isFree) {
      return;
    }

    setIsProcessing(true);

    try {
      const userRef = doc(db, "users", user.uid);
      
      if (plan.isTrial) {
        // Handle trial plan selection
        const currentDate = new Date();
        const trialDuration = plan.trialDays || 7;
        const expirationDate = new Date(currentDate.getTime() + (trialDuration * 24 * 60 * 60 * 1000));

        await updateDoc(userRef, {
          plan: plan.name,
          isActive: true,
          planExpiresAt: expirationDate,
          hasUsedTrial: true,
          trialUsedAt: currentDate,
          planUpdatedAt: currentDate,
        }).catch(async () => {
          await setDoc(userRef, {
            plan: plan.name,
            isActive: true,
            planExpiresAt: expirationDate,
            hasUsedTrial: true,
            trialUsedAt: currentDate,
            planUpdatedAt: currentDate,
          }, { merge: true });
        });

        const remainingDays = getRemainingDays(expirationDate);
        setUserPlanInfo({
          planName: plan.name,
          expiresAt: expirationDate,
          isActive: true,
          remainingDays,
          hasUsedTrial: true,
          trialUsedAt: currentDate,
        });

        setSelectedPlan(plan);
        alert(`🎉 Welcome to your ${trialDuration}-day free trial of ${plan.name}!`);
      } else if (plan.isFree) {
        // Handle free plan selection
        await updateDoc(userRef, { 
          plan: plan.name,
          isActive: true,
          planUpdatedAt: new Date(),
        }).catch(async () => {
          await setDoc(userRef, { 
            plan: plan.name,
            isActive: true,
            planUpdatedAt: new Date(),
          }, { merge: true });
        });

        setSelectedPlan(plan);
        setUserPlanInfo(prev => ({
          ...prev,
          planName: plan.name,
          isActive: true,
        }));
        alert(`You are now on the ${plan.name}`);
      } else {
        // Paid plan - just set as selected for checkout
        setSelectedPlan(plan);
      }
    } catch (error) {
      console.error("Error selecting plan:", error);
      alert("An error occurred while selecting the plan. Please try again.");
    } finally {
      setIsProcessing(false);
    }
  };

  const handleCheckout = () => {
    if (!selectedPlan || !isRegistered || selectedPlan.isTrial || selectedPlan.isFree) return;
    router.push(`/checkout?plan=${encodeURIComponent(selectedPlan.id)}`);
  };

  const getPlanButtonText = (plan: Plan) => {
    if (!isRegistered) return "Register to Select";
    if (plan.isTrial && userPlanInfo?.hasUsedTrial) return "Trial Used";
    if (selectedPlan?.id === plan.id) return "Current Plan";
    if (plan.isTrial) return "Start Free Trial";
    if (plan.isFree) return "Select Plan";
    return "Select Plan";
  };

  const isPlanButtonDisabled = (plan: Plan) => {
    return !isRegistered || 
           (plan.isTrial && userPlanInfo?.hasUsedTrial) ||
           (selectedPlan?.id === plan.id && (plan.isFree || plan.isTrial)) ||
           isProcessing;
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-pink-600 mx-auto mb-4"></div>
          <p className="text-gray-600">Loading plans...</p>
        </div>
      </div>
    );
  }

  return (
    <section className="bg-gradient-to-b from-pink-50 to-white py-20 px-6 min-h-screen">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="text-center mb-16">
          <h1 className="text-5xl font-bold text-pink-600 mb-4">Choose Your Plan</h1>
          <p className="text-gray-700 text-lg max-w-2xl mx-auto">
            Select the subscription plan that fits your needs and enjoy safe, curated content for your kids.
          </p>
        </div>

        {/* Current Plan Status */}
        {userPlanInfo && isRegistered && (
          <div className={`max-w-3xl mx-auto mb-12 p-6 rounded-xl shadow-md ${
            userPlanInfo.isActive && userPlanInfo.remainingDays 
              ? userPlanInfo.remainingDays <= 3 
                ? "bg-red-50 border-2 border-red-300" 
                : "bg-green-50 border-2 border-green-300"
              : "bg-gray-50 border-2 border-gray-300"
          }`}>
            <div className="flex items-center justify-between flex-wrap gap-4">
              <div>
                <h3 className="text-xl font-bold text-gray-900 mb-2">Current Plan Status</h3>
                <p className="text-gray-700">
                  Active Plan: <span className="font-bold text-pink-600">{userPlanInfo.planName}</span>
                </p>
                
                {userPlanInfo.hasUsedTrial && (
                  <p className="text-sm text-gray-600 mt-1">
                    ✅ Free trial used
                    {userPlanInfo.trialUsedAt && 
                      ` on ${new Date(userPlanInfo.trialUsedAt.seconds * 1000).toLocaleDateString()}`
                    }
                  </p>
                )}
              </div>
              
              <div className="text-right">
                {userPlanInfo.remainingDays !== undefined && userPlanInfo.remainingDays > 0 && (
                  <div className={`text-lg font-semibold ${
                    userPlanInfo.remainingDays <= 3 ? "text-red-600" : "text-green-600"
                  }`}>
                    {userPlanInfo.remainingDays <= 3 
                      ? `⚠️ ${userPlanInfo.remainingDays} day${userPlanInfo.remainingDays === 1 ? '' : 's'} left`
                      : `${userPlanInfo.remainingDays} days remaining`
                    }
                  </div>
                )}
                
                {userPlanInfo.planName !== "Free Plan" && (!userPlanInfo.isActive || userPlanInfo.remainingDays === 0) && (
                  <p className="text-red-600 font-semibold">
                    🚫 Expired - Moved to Free Plan
                  </p>
                )}
              </div>
            </div>
          </div>
        )}

        {/* Plans Grid */}
        <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-8 mb-12">
          {plans.map((plan) => {
            const isCurrentPlan = selectedPlan?.id === plan.id;
            const isRecommended = plan.recommended;
            
            return (
              <div
                key={plan.id}
                className={`relative p-8 rounded-2xl shadow-lg border-2 transition-all duration-300 ${
                  isCurrentPlan
                    ? "border-pink-600 scale-105 shadow-2xl"
                    : plan.isTrial
                    ? "border-purple-300 hover:scale-105 bg-gradient-to-br from-purple-50 to-pink-50"
                    : isRecommended
                    ? "border-blue-300 hover:scale-105 bg-gradient-to-br from-blue-50 to-white"
                    : "border-gray-200 hover:scale-105 bg-white"
                }`}
              >
                {/* Badges */}
                <div className="absolute -top-3 left-0 right-0 flex justify-between px-3">
                  {isCurrentPlan && (
                    <span className="bg-pink-600 text-white px-3 py-1 rounded-full text-sm font-semibold shadow-md">
                      ✓ Current Plan
                    </span>
                  )}
                  
                  {plan.isTrial && !isCurrentPlan && (
                    <span className="bg-purple-600 text-white px-3 py-1 rounded-full text-xs font-semibold shadow-md">
                      FREE TRIAL
                    </span>
                  )}
                  
                  {isRecommended && !isCurrentPlan && !plan.isTrial && (
                    <span className="bg-blue-600 text-white px-3 py-1 rounded-full text-xs font-semibold shadow-md ml-auto">
                      RECOMMENDED
                    </span>
                  )}
                </div>

                {/* Plan Content */}
                <div className="mt-4">
                  <h2 className={`text-2xl font-bold mb-2 ${
                    plan.isTrial ? "text-purple-600" : isRecommended ? "text-blue-600" : "text-pink-600"
                  }`}>
                    {plan.name}
                  </h2>
                  
                  {plan.description && (
                    <p className="text-gray-600 text-sm mb-4">{plan.description}</p>
                  )}
                  
                  <div className="mb-6">
                    <p className="text-gray-900 text-4xl font-bold">
                      {plan.isTrial ? "FREE" : `$${plan.price.toFixed(2)}`}
                    </p>
                    
                    {plan.isTrial ? (
                      <p className="text-purple-600 text-sm font-semibold mt-1">
                        {plan.trialDays || 7} days free trial
                      </p>
                    ) : !plan.isFree && plan.price > 0 ? (
                      <p className="text-gray-600 text-sm mt-1">per month</p>
                    ) : null}
                  </div>

                  <ul className="text-gray-700 mb-8 space-y-3">
                    {plan.features.map((feature, idx) => (
                      <li key={idx} className="flex items-start">
                        <span className={`mr-2 text-lg font-bold ${
                          plan.isTrial ? "text-purple-600" : isRecommended ? "text-blue-600" : "text-pink-600"
                        }`}>
                          ✓
                        </span> 
                        <span className="text-sm">{feature}</span>
                      </li>
                    ))}
                  </ul>

                  <button
                    onClick={() => handleSelectPlan(plan)}
                    disabled={isPlanButtonDisabled(plan)}
                    className={`w-full py-3 rounded-lg font-semibold transition-all duration-300 ${
                      isPlanButtonDisabled(plan)
                        ? "bg-gray-300 text-gray-500 cursor-not-allowed"
                        : isCurrentPlan
                        ? plan.isTrial 
                          ? "bg-purple-700 text-white"
                          : "bg-pink-700 text-white"
                        : plan.isTrial
                        ? "bg-purple-600 text-white hover:bg-purple-700 shadow-md hover:shadow-lg"
                        : isRecommended
                        ? "bg-blue-600 text-white hover:bg-blue-700 shadow-md hover:shadow-lg"
                        : "bg-pink-600 text-white hover:bg-pink-700 shadow-md hover:shadow-lg"
                    }`}
                  >
                    {isProcessing ? "Processing..." : getPlanButtonText(plan)}
                  </button>
                </div>
              </div>
            );
          })}
        </div>

        {/* Checkout Button */}
        {selectedPlan && !selectedPlan.isFree && !selectedPlan.isTrial && isRegistered && (
          <div className="text-center">
            <button
              onClick={handleCheckout}
              disabled={isProcessing}
              className="px-8 py-4 bg-green-600 hover:bg-green-700 disabled:bg-gray-400 text-white font-bold rounded-lg shadow-lg hover:shadow-xl transition-all duration-300 text-lg"
            >
              {isProcessing ? "Processing..." : "Proceed to Checkout →"}
            </button>
            <p className="text-gray-600 text-sm mt-3">Secure payment powered by Stripe</p>
          </div>
        )}

        {/* Not Registered Message */}
        {!isRegistered && user && (
          <div className="max-w-2xl mx-auto mt-12 p-6 bg-yellow-50 border-2 border-yellow-300 rounded-xl text-center">
            <p className="text-gray-700 font-semibold">
              Please complete your registration to select a plan
            </p>
          </div>
        )}
      </div>
    </section>
  );
}