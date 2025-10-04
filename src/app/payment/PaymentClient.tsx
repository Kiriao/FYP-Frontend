'use client';

import { useSearchParams, useRouter } from "next/navigation";
import { query, where, getDocs, Timestamp } from "firebase/firestore";
import { useState, useEffect, useCallback } from "react";
import { auth, db } from "@/lib/firebase";
import { doc, updateDoc, getDoc, addDoc, collection, serverTimestamp } from "firebase/firestore";

interface PlanInfo {
  name: string;
  price: number;
  id: string;
}

interface PromoCode {
  code: string;
  discountType: 'percentage' | 'fixed';
  discountValue: number;
  isActive: boolean;
  expiresAt?: Timestamp | null;
  usageLimit?: number;
  usedCount?: number;
  minPurchase?: number;
}

type PaymentMethod = "Credit/Debit Card" | "PayNow QR Code";

export default function PaymentClient() {
  const searchParams = useSearchParams();
  const router = useRouter();
  const planName = searchParams?.get("plan") ?? "Unknown Plan";
  const planId = searchParams?.get("planId") ?? "";

  const [selectedMethod, setSelectedMethod] = useState<PaymentMethod>("Credit/Debit Card");
  const [processing, setProcessing] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [planInfo, setPlanInfo] = useState<PlanInfo | null>(null);

  // Promo code states
  const [promoCode, setPromoCode] = useState("");
  const [appliedPromo, setAppliedPromo] = useState<PromoCode | null>(null);
  const [promoError, setPromoError] = useState("");
  const [promoLoading, setPromoLoading] = useState(false);
  const [discount, setDiscount] = useState(0);
  const [finalPrice, setFinalPrice] = useState(0);
  const [promoDocId, setPromoDocId] = useState<string | null>(null);

  // Card details
  const [cardName, setCardName] = useState("");
  const [cardNumber, setCardNumber] = useState("");
  const [expiry, setExpiry] = useState("");
  const [cvv, setCvv] = useState("");

  // Form validation errors
  const [errors, setErrors] = useState({
    cardName: "",
    cardNumber: "",
    expiry: "",
    cvv: "",
  });

  // QR mapping
  const planQR: Record<string, string> = {
    "Starter Plan": "/qr/starter.jpg",
    "Mid-Tier Plan": "/qr/mid.jpg",
    "Premium Plan": "/qr/premium.jpg",
    "Free Plan": "/qr/free.png",
    "Unknown Plan": "/qr/free.png",
  };

  // Calculate final price based on promo
  useEffect(() => {
    if (planInfo && appliedPromo) {
      let discountAmount = 0;
      if (appliedPromo.discountType === 'percentage') {
        discountAmount = (planInfo.price * appliedPromo.discountValue) / 100;
      } else {
        discountAmount = appliedPromo.discountValue;
      }
      setDiscount(discountAmount);
      setFinalPrice(Math.max(0, planInfo.price - discountAmount));
    } else if (planInfo) {
      setDiscount(0);
      setFinalPrice(planInfo.price);
    }
  }, [planInfo, appliedPromo]);

  // Fetch plan details
  const fetchPlanDetails = useCallback(async () => {
    try {
      setLoading(true);
      
      if (!planId) {
        throw new Error("No plan ID provided");
      }

      const planRef = doc(db, "plans", planId);
      const planSnap = await getDoc(planRef);

      if (planSnap.exists()) {
        const data = planSnap.data();
        setPlanInfo({
          name: data.name || planName,
          price: data.price || 0,
          id: planId,
        });
      } else {
        throw new Error("Plan not found");
      }
    } catch (err: any) {
      console.error("Error fetching plan:", err);
      setError(err.message || "Failed to load plan details");
    } finally {
      setLoading(false);
    }
  }, [planId, planName]);

  useEffect(() => {
    fetchPlanDetails();
  }, [fetchPlanDetails]);

  // Apply promo code
const handleApplyPromo = async () => {
  if (!promoCode.trim()) {
    setPromoError("Please enter a promo code");
    return;
  }

  setPromoLoading(true);
  setPromoError("");

  try {
    // Query the promoCodes collection by the 'code' field
    const promoQuery = query(
      collection(db, "promoCodes"), 
      where("code", "==", promoCode.toUpperCase())
    );
    const promoSnap = await getDocs(promoQuery);

    if (promoSnap.empty) {
      throw new Error("Invalid promo code");
    }

    const promoDoc = promoSnap.docs[0];
    const promoData = promoDoc.data() as PromoCode;

    // Validate promo code
    if (!promoData.isActive) {
      throw new Error("This promo code is no longer active");
    }

    if (promoData.expiresAt) {
      // Handle Firestore Timestamp - check if toDate method exists
      const expiryDate = (promoData.expiresAt as any).toDate 
        ? (promoData.expiresAt as any).toDate() 
        : new Date(promoData.expiresAt as any);
      
      if (expiryDate < new Date()) {
        throw new Error("This promo code has expired");
      }
    }

    if (promoData.usageLimit && promoData.usedCount && promoData.usedCount >= promoData.usageLimit) {
      throw new Error("This promo code has reached its usage limit");
    }

    if (promoData.minPurchase && planInfo && planInfo.price < promoData.minPurchase) {
      throw new Error(`This promo code requires a minimum purchase of $${promoData.minPurchase.toFixed(2)}`);
    }

    // Apply the promo code (store the document ID too)
    setAppliedPromo({
      ...promoData,
      code: promoCode.toUpperCase()
    });
    setPromoDocId(promoDoc.id); 
    
  } catch (err: any) {
    console.error("Promo code error:", err);
    setPromoError(err.message || "Failed to apply promo code");
    setAppliedPromo(null);
  } finally {
    setPromoLoading(false);
  }
};

  // Remove promo code
  const handleRemovePromo = () => {
    setAppliedPromo(null);
    setPromoCode("");
    setPromoError("");
    setDiscount(0);
    setPromoDocId(null); 
  };

  // Format card number with spaces
  const formatCardNumber = (value: string) => {
    const cleaned = value.replace(/\s/g, "");
    const formatted = cleaned.match(/.{1,4}/g)?.join(" ") || cleaned;
    return formatted.substring(0, 19);
  };

  // Format expiry date
  const formatExpiry = (value: string) => {
    const cleaned = value.replace(/\D/g, "");
    if (cleaned.length >= 2) {
      return cleaned.substring(0, 2) + "/" + cleaned.substring(2, 4);
    }
    return cleaned;
  };

  // Validate card name
  const validateCardName = (name: string): string => {
    if (!name.trim()) return "Cardholder name is required";
    if (name.trim().length < 3) return "Name must be at least 3 characters";
    if (!/^[a-zA-Z\s]+$/.test(name)) return "Name can only contain letters";
    return "";
  };

  // Validate card number (basic Luhn algorithm)
  const validateCardNumber = (number: string): string => {
    const cleaned = number.replace(/\s/g, "");
    if (!cleaned) return "Card number is required";
    if (cleaned.length < 13 || cleaned.length > 19) return "Invalid card number length";
    if (!/^\d+$/.test(cleaned)) return "Card number must contain only digits";
    
    let sum = 0;
    let isEven = false;
    for (let i = cleaned.length - 1; i >= 0; i--) {
      let digit = parseInt(cleaned[i]);
      if (isEven) {
        digit *= 2;
        if (digit > 9) digit -= 9;
      }
      sum += digit;
      isEven = !isEven;
    }
    
    if (sum % 10 !== 0) return "Invalid card number";
    return "";
  };

  // Validate expiry date
  const validateExpiry = (exp: string): string => {
    if (!exp) return "Expiry date is required";
    const [month, year] = exp.split("/");
    if (!month || !year) return "Invalid format (use MM/YY)";
    
    const monthNum = parseInt(month);
    const yearNum = parseInt(year);
    
    if (monthNum < 1 || monthNum > 12) return "Invalid month";
    
    const currentYear = new Date().getFullYear() % 100;
    const currentMonth = new Date().getMonth() + 1;
    
    if (yearNum < currentYear || (yearNum === currentYear && monthNum < currentMonth)) {
      return "Card has expired";
    }
    
    return "";
  };

  // Validate CVV
  const validateCVV = (cvvValue: string): string => {
    if (!cvvValue) return "CVV is required";
    if (!/^\d{3,4}$/.test(cvvValue)) return "CVV must be 3-4 digits";
    return "";
  };

  // Handle input changes with validation
  const handleCardNameChange = (value: string) => {
    setCardName(value);
    setErrors(prev => ({ ...prev, cardName: "" }));
  };

  const handleCardNumberChange = (value: string) => {
    const formatted = formatCardNumber(value);
    setCardNumber(formatted);
    setErrors(prev => ({ ...prev, cardNumber: "" }));
  };

  const handleExpiryChange = (value: string) => {
    const formatted = formatExpiry(value);
    setExpiry(formatted);
    setErrors(prev => ({ ...prev, expiry: "" }));
  };

  const handleCVVChange = (value: string) => {
    const cleaned = value.replace(/\D/g, "").substring(0, 4);
    setCvv(cleaned);
    setErrors(prev => ({ ...prev, cvv: "" }));
  };

  // Validate all fields
  const validateAllFields = (): boolean => {
    if (selectedMethod !== "Credit/Debit Card") return true;

    const newErrors = {
      cardName: validateCardName(cardName),
      cardNumber: validateCardNumber(cardNumber),
      expiry: validateExpiry(expiry),
      cvv: validateCVV(cvv),
    };

    setErrors(newErrors);
    return !Object.values(newErrors).some(error => error !== "");
  };

  const handleConfirmPayment = async () => {
    if (!selectedMethod) {
      alert("Please select a payment method first.");
      return;
    }

    if (selectedMethod === "Credit/Debit Card" && !validateAllFields()) {
      return;
    }

    setProcessing(true);
    setError(null);

    try {
      const user = auth.currentUser;
      if (!user) {
        throw new Error("You must be logged in to complete the payment.");
      }

      if (!planInfo) {
        throw new Error("Plan information not available");
      }

      const currentDate = new Date();
      const expirationDate = new Date(currentDate.getTime() + (30 * 24 * 60 * 60 * 1000));

      // Update user's plan
      const userRef = doc(db, "users", user.uid);
      await updateDoc(userRef, {
        plan: planInfo.name,
        paymentMethod: selectedMethod,
        planUpdatedAt: serverTimestamp(),
        planExpiresAt: expirationDate,
        isActive: true,
      });

      // Record payment transaction
      await addDoc(collection(db, "payments"), {
        userId: user.uid,
        userEmail: user.email,
        planId: planInfo.id,
        planName: planInfo.name,
        amount: finalPrice,
        originalAmount: planInfo.price,
        discount: discount,
        promoCode: appliedPromo?.code || null,
        paymentMethod: selectedMethod,
        status: "completed",
        createdAt: serverTimestamp(),
        expiresAt: expirationDate,
        cardLast4: selectedMethod === "Credit/Debit Card" 
          ? cardNumber.replace(/\s/g, "").slice(-4) 
          : null,
      });

      // Update promo code usage if applied
if (appliedPromo && promoDocId) {
  try {
    const promoDocRef = doc(db, "promoCodes", promoDocId);
    const promoDocSnap = await getDoc(promoDocRef);
    
    if (promoDocSnap.exists()) {
      const currentUsed = promoDocSnap.data().usedCount || 0;
      await updateDoc(promoDocRef, {
        usedCount: currentUsed + 1
      });
    }
  } catch (promoError) {
    console.error("Failed to update promo usage:", promoError);
    // Don't fail the payment if promo update fails
  }
}

      // Update subscription record
      await updateDoc(userRef, {
        lastPaymentDate: serverTimestamp(),
        lastPaymentAmount: finalPrice,
        lastPaymentMethod: selectedMethod,
      });

      // Send email notification
      try {
        await fetch("/api/send-email", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            email: user.email,
            plan: planInfo.name,
            method: selectedMethod,
            amount: finalPrice,
            discount: discount,
            promoCode: appliedPromo?.code,
          }),
        });
      } catch (emailError) {
        console.error("Email notification failed:", emailError);
      }

      alert(`✅ Payment successful! Your ${planInfo.name} subscription is now active for 30 days.`);
      router.push("/plans");
    } catch (error: any) {
      console.error("Payment error:", error);
      setError(error.message || "Something went wrong while processing payment.");
    } finally {
      setProcessing(false);
    }
  };

  const handleGoBack = () => {
    router.push("/plans");
  };

  // Loading state
  if (loading) {
    return (
      <main className="flex items-center justify-center min-h-screen bg-gradient-to-b from-green-50 to-white">
        <div className="text-center">
          <div className="animate-spin rounded-full h-16 w-16 border-b-4 border-green-600 mx-auto mb-4"></div>
          <p className="text-gray-600 text-lg">Loading payment details...</p>
        </div>
      </main>
    );
  }

  // Error state
  if (error && !planInfo) {
    return (
      <main className="flex items-center justify-center min-h-screen bg-gradient-to-b from-green-50 to-white px-6">
        <div className="max-w-md mx-auto text-center p-8 rounded-2xl shadow-lg bg-white border-2 border-red-200">
          <div className="w-16 h-16 bg-red-100 rounded-full flex items-center justify-center mx-auto mb-4">
            <span className="text-3xl">⚠️</span>
          </div>
          <h2 className="text-2xl font-bold text-red-600 mb-4">Error</h2>
          <p className="text-gray-700 mb-6">{error}</p>
          <button
            onClick={handleGoBack}
            className="px-6 py-3 bg-green-600 hover:bg-green-700 text-white font-semibold rounded-lg shadow-md transition-all duration-300"
          >
            Back to Plans
          </button>
        </div>
      </main>
    );
  }

  return (
    <section className="bg-gradient-to-b from-green-50 to-white min-h-screen py-20 px-6">
      <div className="max-w-6xl mx-auto">
        {/* Back Button */}
        <button
          onClick={handleGoBack}
          className="mb-6 text-green-600 hover:text-green-700 font-semibold flex items-center transition-colors"
        >
          ← Back to Plans
        </button>

        <div className="bg-white rounded-3xl shadow-2xl border border-gray-200 overflow-hidden">
          <div className="grid lg:grid-cols-3">
            {/* Sidebar - Payment Methods */}
            <div className="lg:col-span-1 bg-gradient-to-b from-gray-50 to-gray-100 border-r p-8">
              <h2 className="text-2xl font-bold text-gray-800 mb-6">Payment Method</h2>
              
              <div className="space-y-4">
                <button
                  onClick={() => setSelectedMethod("Credit/Debit Card")}
                  className={`w-full text-left px-5 py-4 rounded-xl font-semibold transition-all duration-300 flex items-center ${
                    selectedMethod === "Credit/Debit Card"
                      ? "bg-green-600 text-white shadow-lg scale-105"
                      : "bg-white hover:bg-gray-200 text-gray-700 border-2 border-gray-200"
                  }`}
                >
                  <span className="text-2xl mr-3">💳</span>
                  <span>Credit / Debit Card</span>
                </button>
                
                <button
                  onClick={() => setSelectedMethod("PayNow QR Code")}
                  className={`w-full text-left px-5 py-4 rounded-xl font-semibold transition-all duration-300 flex items-center ${
                    selectedMethod === "PayNow QR Code"
                      ? "bg-green-600 text-white shadow-lg scale-105"
                      : "bg-white hover:bg-gray-200 text-gray-700 border-2 border-gray-200"
                  }`}
                >
                  <span className="text-2xl mr-3">📱</span>
                  <span>PayNow QR Code</span>
                </button>
              </div>

              {/* Plan Summary */}
              <div className="mt-8 p-5 bg-white rounded-xl border-2 border-green-200">
                <h3 className="font-bold text-gray-800 mb-3">Order Summary</h3>
                <div className="space-y-2 text-sm">
                  <div className="flex justify-between">
                    <span className="text-gray-600">Plan:</span>
                    <span className="font-semibold text-gray-800">{planInfo?.name}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-600">Duration:</span>
                    <span className="font-semibold text-gray-800">30 days</span>
                  </div>
                  <div className="flex justify-between">
                    <span className="text-gray-600">Original Price:</span>
                    <span className={`font-semibold ${appliedPromo ? 'line-through text-gray-500' : 'text-gray-800'}`}>
                      ${planInfo?.price.toFixed(2)}
                    </span>
                  </div>
                  
                  {appliedPromo && (
                    <>
                      <div className="flex justify-between text-green-600">
                        <span className="font-semibold">Discount:</span>
                        <span className="font-semibold">-${discount.toFixed(2)}</span>
                      </div>
                      <div className="flex items-center justify-between text-xs bg-green-50 px-2 py-1 rounded">
                        <span className="font-semibold text-green-700">
                          {appliedPromo.code}
                        </span>
                        <span className="text-green-600">
                          {appliedPromo.discountType === 'percentage' 
                            ? `${appliedPromo.discountValue}% off` 
                            : `$${appliedPromo.discountValue} off`}
                        </span>
                      </div>
                    </>
                  )}
                  
                  <div className="border-t pt-2 mt-2 flex justify-between">
                    <span className="text-gray-800 font-bold">Total:</span>
                    <span className="text-2xl font-bold text-green-600">
                      ${finalPrice.toFixed(2)}
                    </span>
                  </div>
                </div>
              </div>

              {/* Security Badge */}
              <div className="mt-6 p-4 bg-blue-50 rounded-xl border border-blue-200">
                <div className="flex items-start">
                  <span className="text-xl mr-2">🔒</span>
                  <div>
                    <p className="text-xs text-blue-900 font-semibold">Secure Payment</p>
                    <p className="text-xs text-blue-700">256-bit SSL encrypted</p>
                  </div>
                </div>
              </div>
            </div>

            {/* Main Content - Payment Form */}
            <div className="lg:col-span-2 p-8 lg:p-10">
              <div className="max-w-xl mx-auto">
                <h1 className="text-3xl font-bold text-green-600 mb-2 text-center">
                  Complete Payment
                </h1>
                <p className="text-gray-600 text-center mb-8">
                  Secure checkout for {planInfo?.name}
                </p>

                {/* Error Message */}
                {error && (
                  <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-lg">
                    <p className="text-red-700 text-sm">{error}</p>
                  </div>
                )}

                {/* Promo Code Section */}
                <div className="mb-6 p-5 bg-gradient-to-r from-purple-50 to-pink-50 rounded-xl border-2 border-purple-200">
                  <h3 className="font-bold text-gray-800 mb-3 flex items-center">
                    <span className="text-xl mr-2">🎟️</span>
                    Have a Promo Code?
                  </h3>
                  
                  {!appliedPromo ? (
                    <div className="flex gap-2">
                      <input
                        type="text"
                        value={promoCode}
                        onChange={(e) => setPromoCode(e.target.value.toUpperCase())}
                        placeholder="Enter code"
                        className="flex-1 border-2 border-gray-300 px-4 py-2 rounded-lg text-gray-900 focus:outline-none focus:ring-2 focus:ring-purple-500 uppercase"
                        disabled={promoLoading}
                      />
                      <button
                        onClick={handleApplyPromo}
                        disabled={promoLoading || !promoCode.trim()}
                        className="px-6 py-2 bg-purple-600 hover:bg-purple-700 text-white font-semibold rounded-lg disabled:bg-gray-400 disabled:cursor-not-allowed transition-colors"
                      >
                        {promoLoading ? "..." : "Apply"}
                      </button>
                    </div>
                  ) : (
                    <div className="flex items-center justify-between bg-white px-4 py-3 rounded-lg border-2 border-green-300">
                      <div>
                        <p className="font-bold text-green-700">{appliedPromo.code}</p>
                        <p className="text-sm text-green-600">
                          {appliedPromo.discountType === 'percentage' 
                            ? `${appliedPromo.discountValue}% discount applied` 
                            : `$${appliedPromo.discountValue} discount applied`}
                        </p>
                      </div>
                      <button
                        onClick={handleRemovePromo}
                        className="text-red-600 hover:text-red-700 font-semibold text-sm"
                      >
                        Remove
                      </button>
                    </div>
                  )}
                  
                  {promoError && (
                    <p className="text-red-600 text-sm mt-2">{promoError}</p>
                  )}
                </div>

                {/* Credit Card Form */}
                {selectedMethod === "Credit/Debit Card" && (
                  <div className="space-y-5">
                    <div>
                      <label className="block text-sm font-semibold text-gray-700 mb-2">
                        Cardholder Name
                      </label>
                      <input
                        type="text"
                        value={cardName}
                        onChange={(e) => handleCardNameChange(e.target.value)}
                        placeholder="John Doe"
                        className={`w-full border-2 px-4 py-3 text-gray-900 rounded-xl focus:outline-none focus:ring-2 focus:ring-green-500 transition ${
                          errors.cardName ? "border-red-500" : "border-gray-300"
                        }`}
                      />
                      {errors.cardName && (
                        <p className="text-red-500 text-xs mt-1">{errors.cardName}</p>
                      )}
                    </div>

                    <div>
                      <label className="block text-sm font-semibold text-gray-700 mb-2">
                        Card Number
                      </label>
                      <input
                        type="text"
                        value={cardNumber}
                        onChange={(e) => handleCardNumberChange(e.target.value)}
                        placeholder="1234 5678 9012 3456"
                        className={`w-full border-2 text-gray-900 px-4 py-3 rounded-xl focus:outline-none focus:ring-2 focus:ring-green-500 transition ${
                          errors.cardNumber ? "border-red-500" : "border-gray-300"
                        }`}
                        maxLength={19}
                      />
                      {errors.cardNumber && (
                        <p className="text-red-500 text-xs mt-1">{errors.cardNumber}</p>
                      )}
                    </div>

                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <label className="block text-sm font-semibold text-gray-700 mb-2">
                          Expiry Date
                        </label>
                        <input
                          type="text"
                          value={expiry}
                          onChange={(e) => handleExpiryChange(e.target.value)}
                          placeholder="MM/YY"
                          className={`w-full border-2 text-gray-900 px-4 py-3 rounded-xl focus:outline-none focus:ring-2 focus:ring-green-500 transition ${
                            errors.expiry ? "border-red-500" : "border-gray-300"
                          }`}
                          maxLength={5}
                        />
                        {errors.expiry && (
                          <p className="text-red-500 text-xs mt-1">{errors.expiry}</p>
                        )}
                      </div>

                      <div>
                        <label className="block text-sm font-semibold text-gray-700 mb-2">
                          CVV
                        </label>
                        <input
                          type="password"
                          value={cvv}
                          onChange={(e) => handleCVVChange(e.target.value)}
                          placeholder="•••"
                          className={`w-full border-2 px-4 py-3 text-gray-900 rounded-xl focus:outline-none focus:ring-2 focus:ring-green-500 transition ${
                            errors.cvv ? "border-red-500" : "border-gray-300"
                          }`}
                          maxLength={4}
                        />
                        {errors.cvv && (
                          <p className="text-red-500 text-xs mt-1">{errors.cvv}</p>
                        )}
                      </div>
                    </div>

                    <div className="flex items-center p-4 bg-gray-50 rounded-xl border border-gray-200">
                      <span className="text-sm text-gray-600">
                        💡 Your card information is secure and encrypted
                      </span>
                    </div>
                  </div>
                )}

                {/* PayNow QR Code */}
                {selectedMethod === "PayNow QR Code" && (
                  <div className="space-y-6">
                    <div className="text-center">
                      <p className="text-gray-700 font-semibold mb-4">
                        Scan QR Code to Pay ${finalPrice.toFixed(2)}
                      </p>
                      <div className="inline-block p-4 bg-white border-2 border-gray-300 rounded-2xl shadow-lg">
                        <img
                          src={planQR[planInfo?.name || "Unknown Plan"] ?? "/qr/free.png"}
                          alt={`${planInfo?.name} PayNow QR Code`}
                          className="w-64 h-64 object-contain"
                        />
                      </div>
                      <p className="text-sm text-gray-600 mt-4">
                        After payment, click "Confirm Payment" below
                      </p>
                    </div>

                    <div className="p-4 bg-blue-50 rounded-xl border border-blue-200">
                      <h4 className="font-semibold text-blue-900 mb-2 text-sm">
                        How to pay with PayNow:
                      </h4>
                      <ol className="text-sm text-blue-800 space-y-1 list-decimal list-inside">
                        <li>Open your banking app</li>
                        <li>Scan the QR code above</li>
                        <li>Verify the amount and confirm payment</li>
                        <li>Return here and click "Confirm Payment"</li>
                      </ol>
                    </div>
                  </div>
                )}

                {/* Action Buttons */}
                <div className="mt-8 space-y-3">
                  <button
                    onClick={handleConfirmPayment}
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
                        Processing Payment...
                      </span>
                    ) : (
                      `Pay ${finalPrice.toFixed(2)}`
                    )}
                  </button>

                  <button
                    onClick={handleGoBack}
                    disabled={processing}
                    className="w-full py-3 rounded-xl font-semibold text-gray-600 hover:text-gray-800 hover:bg-gray-100 transition-all duration-300 border-2 border-gray-300"
                  >
                    Cancel Payment
                  </button>
                </div>

                <p className="text-center text-xs text-gray-500 mt-6">
                  By completing this payment, you agree to our Terms of Service
                </p>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}