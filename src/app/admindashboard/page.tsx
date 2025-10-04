'use client';

import { useEffect, useState } from "react";
import { auth, db } from "@/lib/firebase";
import {
  collection,
  doc,
  getDoc,
  getDocs,
  query,
  orderBy,
  limit,
  where,
  Timestamp,
} from "firebase/firestore";
import { onAuthStateChanged, User } from "firebase/auth";
import AnalyticsDashboard from "../AnalyticsDashboard/AnalyticsDashboard";

// Import all the tab components
import AdminSidebar from "../components/admin/AdminSidebar";
import UsersTab from "../components/admin/UsersTab";
import PlansTab from "../components/admin/PlansTab";
import SubscriptionsTab from "../components/admin/SubscriptionsTab";
import PromoTab from "../components/admin/PromoTab";
import ReviewsTab from "../components/admin/ReviewsTab";
import ReportsTab from "../components/admin/ReportsTab";
import ReportedContentTab from "../components/admin/ReportedContentTab";
import FAQsTab from "../components/admin/FAQsTab";
import UserQuestionsTab from "../components/admin/UserQuestionsTab";
import ContactsTab from "../components/admin/ContactsTab";
import { RefreshCw, TrendingUp, Users, MessageSquare, AlertTriangle } from "lucide-react";

// Interfaces
interface UserData { uid: string; fullName: string; email: string; role?: string; plan?: string; createdAt?: any; }
interface ReviewData { uid: string; userName: string; message: string; showOnHome?: boolean; createdAt?: any; }
interface ContactData { uid: string; name: string; email: string; message: string; createdAt: any; status?: string; }
interface PlanData { id: string; name: string; price: number; description: string; features: string[]; }
interface PlanSummary { plan: string; count: number; }
interface PromoCode { id: string; code: string; discountType: 'percentage' | 'fixed'; discountValue: number; isActive: boolean; expiresAt?: any; usageLimit?: number; usedCount?: number; minPurchase?: number; createdAt?: any; description?: string; }
interface Review { id: string; userId: string; userName: string; content: string; itemId: string; type: string; title: string; }
interface Report { id: string; reviewId?: string; reportedBy: string; reason?: string; createdAt: any; reviewData?: Review; title?: string; itemId?: string; type?: string; status?: string; }
interface FAQItem { id: string; question: string; answer: string; category?: string; createdAt: any; updatedAt: any; }
interface UserQuestion { id: string; question: string; userEmail: string; userName: string; answered: boolean; answer?: string; createdAt: any; answeredAt?: any; }

interface DashboardStats {
  totalUsers: number;
  newUsersThisWeek: number;
  totalReviews: number;
  pendingReports: number;
  unansweredQuestions: number;
  recentContacts: number;
}

export default function AdminDashboard() {
  const [users, setUsers] = useState<UserData[]>([]);
  const [reviews, setReviews] = useState<ReviewData[]>([]);
  const [contacts, setContacts] = useState<ContactData[]>([]);
  const [plans, setPlans] = useState<PlanData[]>([]);
  const [planSummary, setPlanSummary] = useState<PlanSummary[]>([]);
  const [subscriptions, setSubscriptions] = useState<any[]>([]);
  const [promoCodes, setPromoCodes] = useState<PromoCode[]>([]);
  const [reports, setReports] = useState<Report[]>([]);
  const [reportedContent, setReportedContent] = useState<Report[]>([]);
  const [faqs, setFaqs] = useState<FAQItem[]>([]);
  const [userQuestions, setUserQuestions] = useState<UserQuestion[]>([]);
  const [currentUser, setCurrentUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState("");
  const [activeTab,setActiveTab] = useState<string>("overview");
  const [stats, setStats] = useState<DashboardStats>({
    totalUsers: 0,
    newUsersThisWeek: 0,
    totalReviews: 0,
    pendingReports: 0,
    unansweredQuestions: 0,
    recentContacts: 0,
  });
  const [newFAQForm, setNewFAQForm] = useState({ question: "", answer: "", category: "general" });

  // Calculate statistics
  const calculateStats = (
    usersList: UserData[],
    reviewsList: ReviewData[],
    reportsList: Report[],
    reportedContentList: Report[],
    questionsList: UserQuestion[],
    contactsList: ContactData[],
    promoCodesList: PromoCode[]
  ) => {
    const oneWeekAgo = new Date();
    oneWeekAgo.setDate(oneWeekAgo.getDate() - 7);

    const newUsers = usersList.filter(u => {
      if (!u.createdAt) return false;
      const createdDate = u.createdAt.toDate ? u.createdAt.toDate() : new Date(u.createdAt);
      return createdDate >= oneWeekAgo;
    }).length;

    const pendingReportsCount = reportsList.filter(r => r.status !== 'resolved').length + 
                                 reportedContentList.filter(r => r.status !== 'resolved').length;

    const unanswered = questionsList.filter(q => !q.answered).length;

    const recentContactsCount = contactsList.filter(c => {
      if (!c.createdAt) return false;
      const contactDate = c.createdAt.toDate ? c.createdAt.toDate() : new Date(c.createdAt);
      return contactDate >= oneWeekAgo;
    }).length;

    setStats({
      totalUsers: usersList.length,
      newUsersThisWeek: newUsers,
      totalReviews: reviewsList.length,
      pendingReports: pendingReportsCount,
      unansweredQuestions: unanswered,
      recentContacts: recentContactsCount,
    });
  };

  // Fetch Functions with error handling
  const fetchUsers = async () => {
    try {
      const q = query(collection(db, "users"), orderBy("createdAt", "desc"));
      const snap = await getDocs(q);
      const list: UserData[] = snap.docs.map(doc => ({ 
        ...(doc.data() as UserData), 
        uid: doc.id 
      }));
      setUsers(list);

      const counts: Record<string, number> = {};
      list.forEach(u => { 
        if (u.plan) counts[u.plan] = (counts[u.plan] || 0) + 1; 
      });
      setPlanSummary(Object.entries(counts).map(([plan, count]) => ({ plan, count })));
      
      return list;
    } catch (err) {
      console.error("Error fetching users:", err);
      return [];
    }
  };

  const fetchReviews = async () => {
    try {
      const q = query(collection(db, "reviews"), orderBy("createdAt", "desc"));
      const snap = await getDocs(q);
      const list = snap.docs.map(doc => ({ 
        ...(doc.data() as ReviewData), 
        uid: doc.id 
      }));
      setReviews(list);
      return list;
    } catch (err) {
      console.error("Error fetching reviews:", err);
      return [];
    }
  };

  const fetchContacts = async () => {
    try {
      const q = query(collection(db, "contacts"), orderBy("createdAt", "desc"));
      const snap = await getDocs(q);
      const list = snap.docs.map(doc => ({ 
        ...(doc.data() as ContactData), 
        uid: doc.id 
      }));
      setContacts(list);
      return list;
    } catch (err) {
      console.error("Error fetching contacts:", err);
      return [];
    }
  };

  const fetchPlans = async () => {
    try {
      const snap = await getDocs(collection(db, "plans"));
      const list = snap.docs.map(doc => ({ 
        ...(doc.data() as PlanData), 
        id: doc.id 
      }));
      setPlans(list);
      return list;
    } catch (err) {
      console.error("Error fetching plans:", err);
      return [];
    }
  };

  const fetchSubscriptions = async () => {
    try {
      const snapshot = await getDocs(collection(db, "users"));
      const subsList = snapshot.docs.map((docSnap) => ({
        id: docSnap.id,
        ...docSnap.data(),
      }));
      setSubscriptions(subsList);
      return subsList;
    } catch (err) {
      console.error("Error fetching subscriptions:", err);
      return [];
    }
  };

  const fetchPromoCodes = async () => {
  try {
    const q = query(collection(db, "promoCodes"), orderBy("createdAt", "desc"));
    const snap = await getDocs(q);
    const list = snap.docs.map(doc => ({ 
      id: doc.id, 
      ...doc.data() 
    } as PromoCode));
    setPromoCodes(list);
    return list;
  } catch (err) {
    console.error("Error fetching promo codes:", err);
    return [];
  }
};

  const fetchReports = async () => {
    try {
      const q = query(collection(db, "reports"), orderBy("createdAt", "desc"));
      const snap = await getDocs(q);
      const temp: Report[] = [];
      
      for (const d of snap.docs) {
        const data = d.data();
        let reviewData: Review | undefined;
        
        if (data.reviewId) {
          try {
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
          } catch (err) {
            console.error("Error fetching review data:", err);
          }
        }
        
        temp.push({ 
          id: d.id, 
          reviewId: data.reviewId, 
          reportedBy: data.reportedBy, 
          reason: data.reason, 
          createdAt: data.createdAt,
          status: data.status || 'pending',
          reviewData 
        });
      }
      setReports(temp);
      return temp;
    } catch (err) {
      console.error("Error fetching reports:", err);
      return [];
    }
  };

  const fetchReportedContent = async () => {
    try {
      const q = query(collection(db, "reports-contents"), orderBy("createdAt", "desc"));
      const snap = await getDocs(q);
      const list = snap.docs.map(d => ({ 
        id: d.id, 
        reportedBy: d.data().reportedBy, 
        reason: d.data().reason, 
        createdAt: d.data().createdAt, 
        title: d.data().title, 
        itemId: d.data().itemId, 
        type: d.data().type,
        status: d.data().status || 'pending'
      }));
      setReportedContent(list);
      return list;
    } catch (err) {
      console.error("Error fetching reported content:", err);
      return [];
    }
  };

  const fetchFAQs = async () => {
    try {
      const q = query(collection(db, "faqs"), orderBy("createdAt", "desc"));
      const snap = await getDocs(q);
      const list = snap.docs.map(doc => ({ 
        id: doc.id, 
        ...doc.data() 
      } as FAQItem));
      setFaqs(list);
      return list;
    } catch (err) {
      console.error("Error fetching FAQs:", err);
      return [];
    }
  };

  const fetchUserQuestions = async () => {
    try {
      const q = query(collection(db, "user-questions"), orderBy("createdAt", "desc"));
      const snap = await getDocs(q);
      const list = snap.docs.map(doc => ({ 
        id: doc.id, 
        ...doc.data() 
      } as UserQuestion));
      setUserQuestions(list);
      return list;
    } catch (err) {
      console.error("Error fetching user questions:", err);
      return [];
    }
  };

  // Refresh all data
const refreshAllData = async () => {
  setRefreshing(true);
  try {
    const [
      usersList,
      reviewsList,
      contactsList,
      plansList,
      subscriptionsList,
      reportsList,
      reportedContentList,
      faqsList,
      questionsList,
      promoCodesList 
    ] = await Promise.all([
      fetchUsers(),
      fetchReviews(),
      fetchContacts(),
      fetchPlans(),
      fetchSubscriptions(),
      fetchReports(),
      fetchReportedContent(),
      fetchFAQs(),
      fetchUserQuestions(),
      fetchPromoCodes()
    ]);

    calculateStats(usersList, reviewsList, reportsList, reportedContentList, questionsList, contactsList, promoCodesList);
  } catch (err) {
    console.error("Error refreshing data:", err);
  } finally {
    setRefreshing(false);
  }
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

      const [
        usersList,
        reviewsList,
        contactsList,
        plansList,
        subscriptionsList,
        reportsList,
        reportedContentList,
        faqsList,
        questionsList,
        promoCodesList  
      ] = await Promise.all([
        fetchUsers(),
        fetchReviews(),
        fetchContacts(),
        fetchPlans(),
        fetchSubscriptions(),
        fetchReports(),
        fetchReportedContent(),
        fetchFAQs(),
        fetchUserQuestions(),
        fetchPromoCodes()  
      ]);

      calculateStats(usersList, reviewsList, reportsList, reportedContentList, questionsList, contactsList, promoCodesList);
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
      <div className="bg-gradient-to-br from-pink-50 to-purple-50 min-h-screen flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-16 w-16 border-b-4 border-pink-600 mx-auto"></div>
          <p className="mt-6 text-gray-700 font-medium text-lg">Loading admin dashboard...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-white min-h-screen flex items-center justify-center">
        <div className="text-center max-w-md p-8 bg-red-50 rounded-lg">
          <AlertTriangle className="w-16 h-16 text-red-600 mx-auto mb-4" />
          <p className="text-red-600 text-xl font-semibold">{error}</p>
          <p className="text-gray-600 mt-2">Please contact support if you believe this is an error.</p>
        </div>
      </div>
    );
  }

  const StatCard = ({ icon: Icon, label, value, change, color }: any) => (
    <div className="bg-white rounded-xl shadow-md p-6 border-l-4" style={{ borderColor: color }}>
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm text-gray-600 font-medium">{label}</p>
          <p className="text-3xl font-bold text-gray-800 mt-2">{value}</p>
          {change !== undefined && (
            <p className="text-sm text-gray-500 mt-1">{change} this week</p>
          )}
        </div>
        <div className="p-4 rounded-full" style={{ backgroundColor: color + '20' }}>
          <Icon className="w-8 h-8" style={{ color }} />
        </div>
      </div>
    </div>
  );

  const DashboardOverview = () => (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h1 className="text-3xl font-bold text-gray-800">Dashboard Overview</h1>
        <button
          onClick={refreshAllData}
          disabled={refreshing}
          className="flex items-center gap-2 px-4 py-2 bg-pink-600 text-white rounded-lg hover:bg-pink-700 transition-colors disabled:opacity-50"
        >
          <RefreshCw className={`w-4 h-4 ${refreshing ? 'animate-spin' : ''}`} />
          {refreshing ? 'Refreshing...' : 'Refresh Data'}
        </button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        <StatCard
          icon={Users}
          label="Total Users"
          value={stats.totalUsers}
          change={`+${stats.newUsersThisWeek} new`}
          color="#ec4899"
        />
        <StatCard
          icon={MessageSquare}
          label="Total Reviews"
          value={stats.totalReviews}
          color="#8b5cf6"
        />
        <StatCard
          icon={AlertTriangle}
          label="Pending Reports"
          value={stats.pendingReports}
          color="#f59e0b"
        />
        <StatCard
          icon={MessageSquare}
          label="Unanswered Questions"
          value={stats.unansweredQuestions}
          color="#06b6d4"
        />
        <StatCard
          icon={TrendingUp}
          label="Recent Contacts"
          value={stats.recentContacts}
          color="#10b981"
        />
        <StatCard
          icon={Users}
          label="Active Plans"
          value={planSummary.length}
          color="#6366f1"
        />
      </div>

      <div className="bg-white rounded-xl shadow-md p-6 mt-6">
        <h2 className="text-xl font-bold text-gray-800 mb-4">Quick Actions</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <button
            onClick={() => setActiveTab('users')}
            className="p-4 bg-pink-50 hover:bg-pink-100 rounded-lg transition-colors text-center"
          >
            <Users className="w-6 h-6 text-pink-600 mx-auto mb-2" />
            <p className="text-sm font-medium text-gray-700">Manage Users</p>
          </button>
          <button
            onClick={() => setActiveTab('reports')}
            className="p-4 bg-orange-50 hover:bg-orange-100 rounded-lg transition-colors text-center"
          >
            <AlertTriangle className="w-6 h-6 text-orange-600 mx-auto mb-2" />
            <p className="text-sm font-medium text-gray-700">View Reports</p>
          </button>
          <button
            onClick={() => setActiveTab('userQuestions')}
            className="p-4 bg-cyan-50 hover:bg-cyan-100 rounded-lg transition-colors text-center"
          >
            <MessageSquare className="w-6 h-6 text-cyan-600 mx-auto mb-2" />
            <p className="text-sm font-medium text-gray-700">Answer Questions</p>
          </button>
          <button
            onClick={() => setActiveTab('analytics')}
            className="p-4 bg-purple-50 hover:bg-purple-100 rounded-lg transition-colors text-center"
          >
            <TrendingUp className="w-6 h-6 text-purple-600 mx-auto mb-2" />
            <p className="text-sm font-medium text-gray-700">View Analytics</p>
          </button>
        </div>
      </div>
    </div>
  );

  const renderActiveTab = () => {
    switch (activeTab) {
      case "overview":
        return <DashboardOverview />;
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
      case "promoCodes":
        return <PromoTab promoCodes={promoCodes} onRefresh={fetchPromoCodes} />;
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
    <main className="bg-gradient-to-br from-gray-50 to-gray-100 min-h-screen p-6 flex text-gray-800">
      <AdminSidebar activeTab={activeTab} setActiveTab={setActiveTab} />
      <section className="flex-1 overflow-auto ml-6">
        {renderActiveTab()}
      </section>
    </main>
  );
}