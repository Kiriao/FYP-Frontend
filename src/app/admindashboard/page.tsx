'use client';

import { useEffect, useState } from "react";
import { auth, db } from "@/lib/firebase";
import {
  collection,
  doc,
  getDoc,
  getDocs,
} from "firebase/firestore";
import { onAuthStateChanged, User } from "firebase/auth";
import AnalyticsDashboard from "../AnalyticsDashboard/AnalyticsDashboard";

// Import all the tab components
import AdminSidebar from "../components/admin/AdminSidebar";
import UsersTab from "../components/admin/UsersTab";
import PlansTab from "../components/admin/PlansTab";
import SubscriptionsTab from "../components/admin/SubscriptionsTab";
import ReviewsTab from "../components/admin/ReviewsTab";
import ReportsTab from "../components/admin/ReportsTab";
import ReportedContentTab from "../components/admin/ReportedContentTab";
import FAQsTab from "../components/admin/FAQsTab";
import UserQuestionsTab from "../components/admin/UserQuestionsTab";
import ContactsTab from "../components/admin/ContactsTab";

// Keep your existing interfaces here
interface UserData { uid: string; fullName: string; email: string; role?: string; plan?: string; }
interface ReviewData { uid: string; userName: string; message: string; showOnHome?: boolean; }
interface ContactData { uid: string; name: string; email: string; message: string; createdAt: any; }
interface PlanData { id: string; name: string; price: number; description: string; features: string[]; }
interface PlanSummary { plan: string; count: number; }
interface Review { id: string; userId: string; userName: string; content: string; itemId: string; type: string; title: string; }
interface Report { id: string; reviewId?: string; reportedBy: string; reason?: string; createdAt: any; reviewData?: Review; title?: string; itemId?: string; type?: string; }
interface FAQItem { id: string; question: string; answer: string; category?: string; createdAt: any; updatedAt: any; }
interface UserQuestion { id: string; question: string; userEmail: string; userName: string; answered: boolean; answer?: string; createdAt: any; answeredAt?: any; }

export default function AdminDashboard() {
  const [users, setUsers] = useState<UserData[]>([]);
  const [reviews, setReviews] = useState<ReviewData[]>([]);
  const [contacts, setContacts] = useState<ContactData[]>([]);
  const [plans, setPlans] = useState<PlanData[]>([]);
  const [planSummary, setPlanSummary] = useState<PlanSummary[]>([]);
  const [subscriptions, setSubscriptions] = useState<any[]>([]);
  const [reports, setReports] = useState<Report[]>([]);
  const [reportedContent, setReportedContent] = useState<Report[]>([]);
  const [faqs, setFaqs] = useState<FAQItem[]>([]);
  const [userQuestions, setUserQuestions] = useState<UserQuestion[]>([]);
  const [currentUser, setCurrentUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [activeTab, setActiveTab] = useState<string>("users");
  const [newFAQForm, setNewFAQForm] = useState({ question: "", answer: "", category: "general" });

  // Fetch Functions
  const fetchUsers = async () => {
    const snap = await getDocs(collection(db, "users"));
    const list: UserData[] = snap.docs.map(doc => ({ ...(doc.data() as UserData), uid: doc.id }));
    setUsers(list);

    const counts: Record<string, number> = {};
    list.forEach(u => { if (u.plan) counts[u.plan] = (counts[u.plan] || 0) + 1; });
    setPlanSummary(Object.entries(counts).map(([plan, count]) => ({ plan, count })));
  };

  const fetchReviews = async () => {
    const snap = await getDocs(collection(db, "reviews"));
    setReviews(snap.docs.map(doc => ({ ...(doc.data() as ReviewData), uid: doc.id })));
  };

  const fetchContacts = async () => {
    const snap = await getDocs(collection(db, "contacts"));
    setContacts(snap.docs.map(doc => ({ ...(doc.data() as ContactData), uid: doc.id })));
  };

  const fetchPlans = async () => {
    const snap = await getDocs(collection(db, "plans"));
    setPlans(snap.docs.map(doc => ({ ...(doc.data() as PlanData), id: doc.id })));
  };

  const fetchSubscriptions = async () => {
    const snapshot = await getDocs(collection(db, "users"));
    const subsList = snapshot.docs.map((docSnap) => ({
      id: docSnap.id,
      ...docSnap.data(),
    }));
    setSubscriptions(subsList);
  };

  const fetchReports = async () => {
    const snap = await getDocs(collection(db, "reports"));
    const temp: Report[] = [];
    for (const d of snap.docs) {
      const data = d.data();
      let reviewData: Review | undefined;
      if (data.reviewId) {
        const rSnap = await getDoc(doc(db, "books-video-reviews", data.reviewId));
        if (rSnap.exists()) {
          const r = rSnap.data();
          reviewData = { 
            id: rSnap.id, 
            userId: r.userId || "", 
            userName: r.userName || "Anonymous", 
            content: r.content || "", 
            itemId: r.itemId || "", 
            type: r.type || "", 
            title: r.title || "" 
          };
        }
      }
      temp.push({ 
        id: d.id, 
        reviewId: data.reviewId, 
        reportedBy: data.reportedBy, 
        reason: data.reason, 
        createdAt: data.createdAt, 
        reviewData 
      });
    }
    setReports(temp);
  };

  const fetchReportedContent = async () => {
    const snap = await getDocs(collection(db, "reports-contents"));
    setReportedContent(
      snap.docs.map(d => ({ 
        id: d.id, 
        reportedBy: d.data().reportedBy, 
        reason: d.data().reason, 
        createdAt: d.data().createdAt, 
        title: d.data().title, 
        itemId: d.data().itemId, 
        type: d.data().type 
      }))
    );
  };

  const fetchFAQs = async () => {
    const snap = await getDocs(collection(db, "faqs"));
    setFaqs(snap.docs.map(doc => ({ id: doc.id, ...doc.data() } as FAQItem)));
  };

  const fetchUserQuestions = async () => {
    const snap = await getDocs(collection(db, "user-questions"));
    setUserQuestions(snap.docs.map(doc => ({ id: doc.id, ...doc.data() } as UserQuestion)));
  };

  // Auth and initialization
  useEffect(() => {
    const unsub = onAuthStateChanged(auth, (u) => {
      setCurrentUser(u);
    });
    return () => unsub();
  }, []);

  useEffect(() => {
    if (!currentUser) return;

    const fetchAdminData = async () => {
      setLoading(true);
      try {
        const snap = await getDoc(doc(db, "users", currentUser.uid));
        const role = snap.exists() ? snap.data()?.role : null;

        if (role !== "admin") {
          setError("You do not have permission to view this page.");
          return;
        }

        await Promise.all([
          fetchUsers(),
          fetchReviews(),
          fetchContacts(),
          fetchPlans(),
          fetchSubscriptions(),
          fetchReports(),
          fetchReportedContent(),
          fetchFAQs(),
          fetchUserQuestions()
        ]);
      } catch (err) {
        console.error(err);
        setError("Failed to fetch admin data.");
      } finally {
        setLoading(false);
      }
    };

    fetchAdminData();
  }, [currentUser]);

  if (loading) {
    return (
      <div className="bg-white min-h-screen flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-pink-600 mx-auto"></div>
          <p className="mt-4 text-gray-600">Loading admin dashboard...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-white min-h-screen flex items-center justify-center">
        <div className="text-center">
          <p className="text-red-600 text-xl">{error}</p>
        </div>
      </div>
    );
  }

  const renderActiveTab = () => {
    switch (activeTab) {
      case "users":
        return <UsersTab users={users} setUsers={setUsers} />;
      case "reviews":
        return <ReviewsTab reviews={reviews} fetchReviews={fetchReviews} />;
      case "contacts":
        return <ContactsTab contacts={contacts} />;
      case "plans":
        return <PlansTab plans={plans} fetchPlans={fetchPlans} />;
      case "subscriptions":
        return <SubscriptionsTab subscriptions={subscriptions} planSummary={planSummary} plans={plans} />;
      case "reports":
        return <ReportsTab reports={reports} setReports={setReports} />;
      case "reportedContent":
        return <ReportedContentTab reportedContent={reportedContent} setReportedContent={setReportedContent} />;
      case "FAQs":
        return <FAQsTab faqs={faqs} fetchFAQs={fetchFAQs} />;
      case "userQuestions":
        return <UserQuestionsTab 
          userQuestions={userQuestions} 
          fetchUserQuestions={fetchUserQuestions}
          setNewFAQForm={setNewFAQForm}
          setActiveTab={setActiveTab}
        />;
      case "analytics":
        return (
          <section>
            <AnalyticsDashboard
              users={users}
              reviews={reviews}
              contacts={contacts}
              plans={plans}
              reports={reports}
              reportedContent={reportedContent}
              subscriptions={subscriptions}
            />
          </section>
        );
      default:
        return <div className="text-center text-gray-500">Select a tab from the sidebar</div>;
    }
  };

  return (
    <main className="bg-white min-h-screen p-6 flex text-gray-800">
      <AdminSidebar activeTab={activeTab} setActiveTab={setActiveTab} />
      <section className="flex-1 overflow-auto">
        {renderActiveTab()}
      </section>
    </main>
  );
}
