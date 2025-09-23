'use client';

import { useState, useEffect } from "react";
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
}

interface UserPlanInfo {
  planName: string;
  expiresAt?: any; // Firestore timestamp
  isActive?: boolean;
  remainingDays?: number;
}

export default function SubscriptionPage() {
  const router = useRouter();
  const [user, setUser] = useState<User | null>(null);
  const [isRegistered, setIsRegistered] = useState(false);
  const [loading, setLoading] = useState(true);
  const [plans, setPlans] = useState<Plan[]>([]);
  const [selectedPlan, setSelectedPlan] = useState<Plan | null>(null);
  const [userPlanInfo, setUserPlanInfo] = useState<UserPlanInfo | null>(null);

  // ✅ Helper functions for expiry checking
  const isPlanExpired = (expirationDate: any): boolean => {
    if (!expirationDate) return true;
    
    const expDate = expirationDate.toDate ? expirationDate.toDate() : new Date(expirationDate);
    return new Date() > expDate;
  };

  const getRemainingDays = (expirationDate: any): number => {
    if (!expirationDate) return 0;
    
    const expDate = expirationDate.toDate ? expirationDate.toDate() : new Date(expirationDate);
    const diffTime = expDate.getTime() - new Date().getTime();
    return Math.max(0, Math.ceil(diffTime / (1000 * 60 * 60 * 24)));
  };

  // ✅ Handle expired plans
  const handleExpiredPlan = async (userRef: any) => {
    console.log("Plan has expired, downgrading to Free Plan");
    
    await updateDoc(userRef, {
      plan: "Free Plan",
      isActive: false,
      planExpiredAt: new Date(),
    });

    return "Free Plan";
  };

  // ✅ Fetch plans from Firestore in ascending order by price
  const fetchPlans = async () => {
    try {
      const q = query(collection(db, "plans"), orderBy("price", "asc"));
      const snapshot = await getDocs(q);

      let plansList: Plan[] = snapshot.docs.map((docSnap) => ({
        id: docSnap.id,
        ...(docSnap.data() as Omit<Plan, "id">),
      }));

      plansList = plansList.sort((a, b) => a.price - b.price);
      setPlans(plansList);
    } catch (error) {
      console.error("Error fetching plans:", error);
    }
  };

  useEffect(() => {
    const unsubscribe = onAuthStateChanged(auth, async (currentUser) => {
      setUser(currentUser);

      if (currentUser) {
        const userRef = doc(db, "users", currentUser.uid);
        const userSnap = await getDoc(userRef);

        if (userSnap.exists()) {
          setIsRegistered(true);

          let currentPlanName = "Free Plan";
          const userData = userSnap.data();
          
          // ✅ Check if user has a plan and if it's expired
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
              });
            } else {
              // Plan has expired or is inactive
              currentPlanName = await handleExpiredPlan(userRef);
              setUserPlanInfo({
                planName: currentPlanName,
                isActive: false,
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
            });
          }

          await fetchPlans();

          setPlans((prev) => {
            const validPlan = prev.find((p) => p.name === currentPlanName);
            setSelectedPlan(validPlan || null);
            return prev;
          });
        } else {
          setIsRegistered(false);
          await fetchPlans();
        }
      } else {
        setIsRegistered(false);
        await fetchPlans();
      }

      setLoading(false);
    });

    return () => unsubscribe();
  }, [router]);

  const handleSelectPlan = async (plan: Plan) => {
    if (!user || !isRegistered) {
      alert("You must be a registered user to select a plan.");
      return;
    }

    setSelectedPlan(plan);

    const userRef = doc(db, "users", user.uid);
    await updateDoc(userRef, { plan: plan.name }).catch(async () => {
      await setDoc(userRef, { plan: plan.name }, { merge: true });
    });

    if (plan.isFree) {
      alert(`You are now on the ${plan.name}`);
      setUserPlanInfo({
        planName: plan.name,
        isActive: true,
      });
    }
  };

  const handleCheckout = () => {
    if (!selectedPlan || !isRegistered) return;
    router.push(`/checkout?plan=${encodeURIComponent(selectedPlan.id)}`);
  };

  if (loading) return <p className="text-center py-20">Loading...</p>;

  return (
    <section className="bg-white py-20 px-6">
      <div className="max-w-6xl mx-auto text-center">
        <h1 className="text-4xl font-bold text-pink-600 mb-6">Choose Your Plan</h1>
        <p className="text-gray-700 mb-12">
          Select the subscription plan that fits your needs and enjoy safe, curated content for your kids.
        </p>

        {/* ✅ Show current plan status */}
        {userPlanInfo && isRegistered && (
          <div className={`mb-8 p-4 rounded-lg ${
            userPlanInfo.isActive && userPlanInfo.remainingDays 
              ? userPlanInfo.remainingDays <= 3 
                ? "bg-red-100 border border-red-300" 
                : "bg-green-100 border border-green-300"
              : "bg-gray-100 border border-gray-300"
          }`}>
            <h3 className="text-lg font-semibold mb-2 text-gray-700">Current Plan Status</h3>
            <p className="text-gray-700">
              You are currently on: <span className="font-bold text-pink-600">{userPlanInfo.planName}</span>
            </p>
            
            {userPlanInfo.remainingDays !== undefined && userPlanInfo.remainingDays > 0 && (
              <p className={`text-sm mt-1 ${
                userPlanInfo.remainingDays <= 3 ? "text-red-600 font-semibold" : "text-gray-600"
              }`}>
                {userPlanInfo.remainingDays <= 3 
                  ? `⚠️ Expires in ${userPlanInfo.remainingDays} day${userPlanInfo.remainingDays === 1 ? '' : 's'}!`
                  : `Valid for ${userPlanInfo.remainingDays} more days`
                }
              </p>
            )}
            
            {userPlanInfo.planName !== "Free Plan" && (!userPlanInfo.isActive || userPlanInfo.remainingDays === 0) && (
              <p className="text-red-600 font-semibold text-sm mt-1">
                🚫 Plan has expired - You've been moved to the Free Plan
              </p>
            )}
          </div>
        )}

        <div className="grid md:grid-cols-4 gap-8">
          {plans.map((plan) => (
            <div
              key={plan.id}
              className={`relative p-8 rounded-2xl shadow-lg border transition-transform duration-300 ${
                selectedPlan?.id === plan.id
                  ? "border-pink-600 scale-[1.03]"
                  : "border-gray-200 hover:scale-[1.02]"
              }`}
            >
              {/* ✅ Show "Current Plan" badge */}
              {selectedPlan?.id === plan.id && (
                <div className="absolute -top-3 left-1/2 transform -translate-x-1/2">
                  <span className="bg-pink-600 text-white px-3 py-1 rounded-full text-sm font-semibold">
                    Current Plan
                  </span>
                </div>
              )}

              <h2 className="text-2xl font-bold text-pink-600 mb-4">{plan.name}</h2>
              <p className="text-gray-900 text-xl font-semibold mb-2">
                $ {plan.price.toFixed(2)}
              </p>
              
              {/* ✅ Show validity period for paid plans */}
              {!plan.isFree && plan.price > 0 && (
                <p className="text-gray-600 text-sm mb-4">Valid for 30 days</p>
              )}

              <ul className="text-gray-700 mb-6 space-y-2">
                {plan.features.map((feature, idx) => (
                  <li key={idx} className="flex items-center">
                    <span className="mr-2 text-pink-600">✔</span> {feature}
                  </li>
                ))}
              </ul>

              <button
                onClick={() => handleSelectPlan(plan)}
                disabled={!isRegistered}
                className={`w-full py-2 rounded-md font-semibold transition ${
                  !isRegistered
                    ? "bg-gray-300 text-gray-500 cursor-not-allowed"
                    : selectedPlan?.id === plan.id
                    ? "bg-pink-700 text-white"
                    : "bg-pink-600 text-white hover:bg-pink-700"
                }`}
              >
                {!isRegistered
                  ? "Register to Select"
                  : selectedPlan?.id === plan.id
                  ? "Selected"
                  : "Select Plan"}
              </button>
            </div>
          ))}
        </div>

        {selectedPlan && !selectedPlan.isFree && isRegistered && (
          <div className="mt-10">
            <button
              onClick={handleCheckout}
              className="px-6 py-3 bg-green-600 hover:bg-green-700 text-white font-bold rounded-lg shadow-lg"
            >
              Proceed to Checkout
            </button>
          </div>
        )}
      </div>
    </section>
  );
}