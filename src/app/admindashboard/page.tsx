'use client';

import AnalyticsDashboard from "../AnalyticsDashboard/AnalyticsDashboard";
import jsPDF from "jspdf";
import autoTable from "jspdf-autotable";
import { useEffect, useState } from "react";
import { auth, db } from "@/lib/firebase";
import {
  collection,
  doc,
  getDoc,
  getDocs,
  updateDoc,
  deleteDoc,
  addDoc,
} from "firebase/firestore";
import { onAuthStateChanged, User } from "firebase/auth";
import {
  PieChart,
  Pie,
  Cell,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import { MdSubscriptions } from "react-icons/md";

// ---------------- Interfaces ----------------
interface UserData { uid: string; fullName: string; email: string; role?: string; plan?: string; }
interface ReviewData { uid: string; userName: string; message: string; showOnHome?: boolean; }
interface ContactData { uid: string; name: string; email: string; message: string; createdAt: any; }
interface PlanData { id: string; name: string; price: number; description: string; features: string[]; }
interface PlanSummary { plan: string; count: number; }
interface Review { id: string; userId: string; userName: string; content: string; itemId: string; type: string; title: string; }
interface Report { id: string; reviewId?: string; reportedBy: string; reason?: string; createdAt: any; reviewData?: Review; title?: string; itemId?: string; type?: string; }

const COLORS = ["#ef4444", "#3b82f6", "#22c55e", "#f59e0b"];

// ---------------- Component ----------------
export default function AdminDashboard() {
  const [users, setUsers] = useState<UserData[]>([]);
  const [reviews, setReviews] = useState<ReviewData[]>([]);
  const [contacts, setContacts] = useState<ContactData[]>([]);
  const [plans, setPlans] = useState<PlanData[]>([]);
  const [planSummary, setPlanSummary] = useState<PlanSummary[]>([]);
  const [reports, setReports] = useState<Report[]>([]);
  const [reportedContent, setReportedContent] = useState<Report[]>([]);
  const [currentUser, setCurrentUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [activeTab, setActiveTab] = useState<"users"|"reviews"|"contacts"|"plans"|"subscriptions"|"reports"|"reportedContent"|"analytics">("users");
  const [editingUser, setEditingUser] = useState<UserData | null>(null);
  const [editForm, setEditForm] = useState({ fullName: "", email: "", role: "", plan: "" });
  const [editingPlan, setEditingPlan] = useState<PlanData | null>(null);
  const [editPlanForm, setEditPlanForm] = useState({ name: "", price: 0, description: "", features: "" });
  const [subscriptions, setSubscriptions] = useState<any[]>([]);
  const [newPlan, setNewPlan] = useState({ name: "", price: 0, description: "", features: "" });

  // ---------------- Fetch Functions ----------------
  const fetchUsers = async () => {
    const snap = await getDocs(collection(db, "users"));
    const list: UserData[] = snap.docs.map(doc => ({ ...(doc.data() as UserData), uid: doc.id }));
    setUsers(list);

    // Plan summary for subscriptions
    const counts: Record<string, number> = {};
    list.forEach(u => { if (u.plan) counts[u.plan] = (counts[u.plan] || 0) + 1; });
    setPlanSummary(Object.entries(counts).map(([plan,count])=>({plan,count})));
  };


  const fetchReviews = async () => {
    const snap = await getDocs(collection(db, "reviews"));
    setReviews(snap.docs.map(doc => ({ ...(doc.data() as ReviewData), uid: doc.id })));
  };

// ---------------- Contact Fetch ----------------

  const fetchContacts = async () => {
    const snap = await getDocs(collection(db, "contacts"));
    setContacts(snap.docs.map(doc => ({ ...(doc.data() as ContactData), uid: doc.id })));
  };

  // ---------------- Plans Fetch ----------------

  const fetchPlans = async () => {
    const snap = await getDocs(collection(db, "plans"));
    setPlans(snap.docs.map(doc => ({ ...(doc.data() as PlanData), id: doc.id })));
  };
// ---------------- Reports Fetch ----------------

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
          reviewData = { id: rSnap.id, userId: r.userId || "", userName: r.userName || "Anonymous", content: r.content || "", itemId: r.itemId || "", type: r.type || "", title: r.title || "" };
        }
      }
      temp.push({ id: d.id, reviewId: data.reviewId, reportedBy: data.reportedBy, reason: data.reason, createdAt: data.createdAt, reviewData });
    }
    setReports(temp);
  };

  // ---------------- Report Content Fetch ----------------

  const fetchReportedContent = async () => {
    const snap = await getDocs(collection(db, "reports-contents"));
    setReportedContent(
      snap.docs.map(d => ({ id: d.id, reportedBy: d.data().reportedBy, reason: d.data().reason, createdAt: d.data().createdAt, title: d.data().title, itemId: d.data().itemId, type: d.data().type }))
    );
  };

  // ---------------- Action Handlers ----------------
  const handleDeleteUser = async (uid: string) => {
  if (!confirm("Are you sure you want to delete this user? This action cannot be undone.")) {
    return;
  }

  await deleteDoc(doc(db, "users", uid));
  setUsers(prev => prev.filter(u => u.uid !== uid));
};
const handleToggleUser = async (uid: string, role?: string) => {
  const newRole = role === "inactive" ? "user" : "inactive";

  if (!confirm(`Are you sure you want to ${newRole === "inactive" ? "deactivate" : "activate"} this account?`)) {
    return;
  }

  await updateDoc(doc(db, "users", uid), { role: newRole });
  setUsers(prev =>
    prev.map(u => (u.uid === uid ? { ...u, role: newRole } : u))
  );
};



  const handleToggleShowOnHome = async (id:string,current:boolean) => { await updateDoc(doc(db,"reviews",id),{showOnHome:!current}); fetchReviews(); };
  const handleAddPlan = async (e:React.FormEvent) => { e.preventDefault(); await addDoc(collection(db,"plans"),{name:newPlan.name,price:newPlan.price,description:newPlan.description,features:newPlan.features.split(",").map(f=>f.trim())}); setNewPlan({name:"",price:0,description:"",features:""}); fetchPlans(); };
  const handleDeletePlan = async (id:string) => { await deleteDoc(doc(db,"plans",id)); fetchPlans(); };
  const handleDeleteReport = async (id:string, collectionName:string="reports") => { await deleteDoc(doc(db,collectionName,id)); if(collectionName==="reports") setReports(prev=>prev.filter(r=>r.id!==id)); else setReportedContent(prev=>prev.filter(r=>r.id!==id)); };
  const handleDeleteReviewAndReport = async (report:Report) => { await deleteDoc(doc(db,"reports",report.id)); if(report.reviewData) await deleteDoc(doc(db,"books-video-reviews",report.reviewId!)); setReports(prev=>prev.filter(r=>r.id!==report.id)); };
  const exportCSV = () => { if(!planSummary.length) return; const header=["Plan","Active Users"]; const rows=planSummary.map(p=>[p.plan,p.count]); const csv=[header,...rows].map(e=>e.join(",")).join("\n"); const blob=new Blob([csv],{type:"text/csv;charset=utf-8;"}); const url=URL.createObjectURL(blob); const link=document.createElement("a"); link.href=url; link.setAttribute("download","subscription_report.csv"); link.click(); URL.revokeObjectURL(url); };

// Export PDF

const fetchSubscriptions = async () => {
  const snapshot = await getDocs(collection(db, "users"));
  const subsList = snapshot.docs.map((docSnap) => ({
    id: docSnap.id,
    ...docSnap.data(),
  }));
  setSubscriptions(subsList);
};

useEffect(() => {
  fetchSubscriptions();
}, []);


const exportPDF = () => {
  if (!subscriptions.length) return;

  const doc = new jsPDF();
  doc.setFontSize(18);
  doc.text("Subscriptions Report", 14, 20);

  const tableColumn = ["Name", "Email", "Plan", "Start Date", "Status"];
  const tableRows: any[] = [];

  subscriptions.forEach((sub) => {
    tableRows.push([
      sub.fullName || "N/A",
      sub.email || "N/A",
      sub.plan || "Free Plan",
      sub.startDate
        ? new Date(sub.startDate.seconds * 1000).toLocaleDateString()
        : "N/A",
      sub.status || "Active",
    ]);
  });

  autoTable(doc, {
    head: [tableColumn],
    body: tableRows,
    startY: 30,
  });

  doc.save("subscriptions-report.pdf");
};

const calculateMRR = (subscriptions: any[], plans: PlanData[]) => {
  let mrr = 0;
  subscriptions.forEach((sub) => {
    const plan = plans.find((p) => p.name === sub.plan);
    if (plan) {
      mrr += plan.price;
    }
  });
  return mrr;
};

const calculateChurn = (subscriptions: any[]) => {
  const total = subscriptions.length;
  const churned = subscriptions.filter((s) => s.status === "canceled").length;
  return total > 0 ? ((churned / total) * 100).toFixed(2) : "0.00";
};



  // ---------------- Auth + Init ----------------
useEffect(() => {
  const unsub = onAuthStateChanged(auth, (u) => {
    setCurrentUser(u);
  });

  return () => unsub();
}, []);

// Fetch all admin data after currentUser is set
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
        fetchReports(),
        fetchReportedContent(),
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



  // ---------------- Render ----------------
  return (
    <main className="bg-white min-h-screen p-6 flex text-gray-800">
      {/* Sidebar */}
      <aside className="w-64 mr-8 border-r border-gray-200">
        <ul className="space-y-2">
         {["users","reviews","contacts","plans","subscriptions","reports","reportedContent","analytics"].map(tab => (
  <li 
    key={tab} 
    className={`cursor-pointer px-4 py-2 rounded ${activeTab===tab?"bg-pink-100 font-semibold":"hover:bg-gray-100"}`} 
    onClick={()=>setActiveTab(tab as any)}
  >
    {tab.charAt(0).toUpperCase()+tab.slice(1)}
  </li>
))}

        </ul>
      </aside>

      {/* Content */}
      <section className="flex-1 overflow-auto">
        {/* Users */}
        {activeTab==="users" && (
          <section>
            <h2 className="text-2xl font-bold text-pink-600 mb-6">Users</h2>
            <table className="w-full border">
              <thead>
                <tr className="bg-gray-100"><th className="p-2">Name</th><th className="p-2">Email</th><th className="p-2">Role</th><th className="p-2">Plan</th><th className="p-2">Actions</th></tr>
              </thead>
              <tbody>{users.map(u=>(
                <tr key={u.uid} className="border-t">
                  <td className="p-2">{u.fullName}</td>
                  <td className="p-2">{u.email}</td>
                  <td className="p-2">{u.role}</td>
                  <td className="p-2">{u.plan||"—"}</td>
                  <td className="p-2 flex gap-2">
                    <button onClick={()=>handleToggleUser(u.uid,u.role)} className="px-2 py-1 bg-blue-500 text-white rounded">{u.role==="inactive"?"Activate":"Deactivate"}</button>
                    <button onClick={()=>handleDeleteUser(u.uid)} className="px-2 py-1 bg-red-500 text-white rounded">Delete</button>
                    <button 
  onClick={() => {
    setEditingUser(u);
    setEditForm({
      fullName: u.fullName,
      email: u.email,
      role: u.role || "user",
      plan: u.plan || "Free Plans"
    });
  }} 
  className="px-2 py-1 bg-yellow-500 text-white rounded"
>
  Edit
</button>
                  </td>
                </tr>
              ))}</tbody>
            </table>
            {editingUser && (
  <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
    <div className="bg-white p-6 rounded-lg shadow-lg w-full max-w-md">
      <h3 className="text-xl font-bold mb-4">Edit User</h3>

      <div className="space-y-3">
        <input
          type="text"
          value={editForm.fullName}
          onChange={(e) => setEditForm({ ...editForm, fullName: e.target.value })}
          placeholder="Full Name"
          className="w-full border p-2 rounded"
        />
        <input
          type="email"
          value={editForm.email}
          onChange={(e) => setEditForm({ ...editForm, email: e.target.value })}
          placeholder="Email"
          className="w-full border p-2 rounded"
        />
        <select
          value={editForm.role}
          onChange={(e) => setEditForm({ ...editForm, role: e.target.value })}
          className="w-full border p-2 rounded"
        >
          <option value="user">User</option>
          <option value="admin">Admin</option>
          <option value="inactive">Inactive</option>
        </select>
        <select
          value={editForm.plan}
          onChange={(e) => setEditForm({ ...editForm, plan: e.target.value })}
          className="w-full border p-2 rounded"
        >
          <option value="Free Plans">Free Plans</option>
          <option value="Basic">Basic</option>
          <option value="Premium">Premium</option>
        </select>
      </div>

      <div className="flex justify-end gap-2 mt-6">
        <button 
          onClick={() => setEditingUser(null)} 
          className="px-4 py-2 bg-gray-300 rounded"
        >
          Cancel
        </button>
        <button 
          onClick={async () => {
            if (!editingUser) return;
            await updateDoc(doc(db, "users", editingUser.uid), {
              fullName: editForm.fullName,
              email: editForm.email,
              role: editForm.role,
              plan: editForm.plan,
            });
            setUsers(prev =>
              prev.map(u => u.uid === editingUser.uid ? { ...u, ...editForm } : u)
            );
            setEditingUser(null);
          }}
          className="px-4 py-2 bg-pink-600 text-white rounded"
        >
          Save
        </button>
      </div>
    </div>
  </div>
)}

          </section>
        )}

        {/* Reviews */}
        {activeTab==="reviews" && (
          <section>
            <h2 className="text-2xl font-bold text-pink-600 mb-6">Reviews</h2>
            <ul className="space-y-4">{reviews.map(r=>(
              <li key={r.uid} className="p-4 border rounded">
                <p><strong>{r.userName}</strong>: {r.message}</p>
                <button onClick={()=>handleToggleShowOnHome(r.uid,!!r.showOnHome)} className="mt-2 px-2 py-1 bg-green-500 text-white rounded">{r.showOnHome?"Hide from Home":"Show on Home"}</button>
              </li>
            ))}</ul>
          </section>
        )}

        {/* Contacts */}
        {activeTab==="contacts" && (
          <section>
            <h2 className="text-2xl font-bold text-pink-600 mb-6">Contacts</h2>
            <ul className="space-y-4">{contacts.map(c=>(
              <li key={c.uid} className="p-4 border rounded">
                <p><strong>{c.name}</strong> ({c.email})</p>
                <p>{c.message}</p>
                <p className="text-sm text-gray-500">{c.createdAt?.toDate?c.createdAt.toDate().toLocaleString():""}</p>
              </li>
            ))}</ul>
          </section>
        )}

        {/* Plans */}
        {activeTab==="plans" && (
          <section>
            <h2 className="text-2xl font-bold text-pink-600 mb-6">Plans</h2>
            <form onSubmit={handleAddPlan} className="space-y-2 mb-6">
              <input value={newPlan.name} onChange={e=>setNewPlan({...newPlan,name:e.target.value})} placeholder="Name" className="border p-2 w-full"/>
              <input type="number" value={newPlan.price} onChange={e=>setNewPlan({...newPlan,price:Number(e.target.value)})} placeholder="Price" className="border p-2 w-full"/>
              <textarea value={newPlan.description} onChange={e=>setNewPlan({...newPlan,description:e.target.value})} placeholder="Description" className="border p-2 w-full"/>
              <input value={newPlan.features} onChange={e=>setNewPlan({...newPlan,features:e.target.value})} placeholder="Features (comma-separated)" className="border p-2 w-full"/>
              <button type="submit" className="px-3 py-1 bg-pink-600 text-white rounded">Add Plan</button>
            </form>
            <ul className="space-y-4">{plans.map(p=>(
<li key={p.id} className="p-4 border rounded flex justify-between items-center">
  <div>
    <p className="font-bold">{p.name} - ${p.price}</p>
    <p>{p.description}</p>
    <ul className="list-disc pl-5 text-sm text-gray-600">
      {p.features.map((f, i) => <li key={i}>{f}</li>)}
    </ul>
  </div>

  {/* Wrap buttons in a flex container */}
  <div className="flex gap-2">
    <button
  onClick={async () => {
    if (!confirm("Are you sure you want to delete this plan? This action cannot be undone.")) {
      return;
    }
    await handleDeletePlan(p.id);
  }}
  className="px-3 py-1 bg-red-500 text-white rounded"
>
  Delete
</button>

    <button
      onClick={() => {
        setEditingPlan(p);
        setEditPlanForm({
          name: p.name,
          price: p.price,
          description: p.description,
          features: p.features.join(", "),
        });
      }}
      className="px-3 py-1 bg-yellow-500 text-white rounded"
    >
      Edit
    </button>
  </div>
</li>

            ))}</ul>
            {editingPlan && (
  <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
    <div className="bg-white p-6 rounded-lg shadow-lg w-full max-w-md">
      <h3 className="text-xl font-bold mb-4">Edit Plan</h3>

      <div className="space-y-3">
        <input
          type="text"
          value={editPlanForm.name}
          onChange={(e) => setEditPlanForm({ ...editPlanForm, name: e.target.value })}
          placeholder="Plan Name"
          className="w-full border p-2 rounded"
        />
        <input
          type="number"
          value={editPlanForm.price}
          onChange={(e) => setEditPlanForm({ ...editPlanForm, price: Number(e.target.value) })}
          placeholder="Price"
          className="w-full border p-2 rounded"
        />
        <textarea
          value={editPlanForm.description}
          onChange={(e) => setEditPlanForm({ ...editPlanForm, description: e.target.value })}
          placeholder="Description"
          className="w-full border p-2 rounded"
        />
        <input
          type="text"
          value={editPlanForm.features}
          onChange={(e) => setEditPlanForm({ ...editPlanForm, features: e.target.value })}
          placeholder="Features (comma-separated)"
          className="w-full border p-2 rounded"
        />
      </div>

      <div className="flex justify-end gap-2 mt-6">
        <button 
          onClick={() => setEditingPlan(null)} 
          className="px-4 py-2 bg-gray-300 rounded"
        >
          Cancel
        </button>
        <button 
          onClick={async () => {
            if (!editingPlan) return;
            await updateDoc(doc(db, "plans", editingPlan.id), {
              name: editPlanForm.name,
              price: editPlanForm.price,
              description: editPlanForm.description,
              features: editPlanForm.features.split(",").map(f => f.trim())
            });
            setPlans(prev => prev.map(p => p.id === editingPlan.id ? { ...p, ...editPlanForm, features: editPlanForm.features.split(",").map(f => f.trim()) } : p));
            setEditingPlan(null);
          }}
          className="px-4 py-2 bg-pink-600 text-white rounded"
        >
          Save
        </button>
      </div>
    </div>
  </div>
)}

          </section>
        )}

        {/* Subscriptions */}
        {activeTab==="subscriptions" && (
          <section>
            <h2 className="text-2xl font-bold text-pink-600 mb-6 flex items-center gap-2"><MdSubscriptions/> Subscriptions</h2>
 {/* KPI Cards */}
  <div className="grid grid-cols-2 md:grid-cols-4 gap-6 mb-8">
    <div className="p-4 bg-white border rounded shadow text-center">
      <p className="text-gray-500">Total Users</p>
      <p className="text-xl font-bold">{subscriptions.length}</p>
    </div>
    <div className="p-4 bg-white border rounded shadow text-center">
      <p className="text-gray-500">Active Plans</p>
      <p className="text-xl font-bold">{planSummary.length}</p>
    </div>
    <div className="p-4 bg-white border rounded shadow text-center">
      <p className="text-gray-500">MRR</p>
      <p className="text-xl font-bold text-green-600">
        ${calculateMRR(subscriptions, plans)}
      </p>
    </div>
    <div className="p-4 bg-white border rounded shadow text-center">
      <p className="text-gray-500">Churn Rate</p>
      <p className="text-xl font-bold text-red-600">
        {calculateChurn(subscriptions)}%
      </p>
    </div>
  </div>

            {planSummary.length===0?<p>No subscriptions yet.</p>:(
              <>
                <div className="grid md:grid-cols-3 gap-6 mb-12">{planSummary.map((p,i)=>(
                  <div key={i} className="p-6 bg-white border rounded shadow text-center"><h3 className="font-bold">{p.plan}</h3><p className="text-2xl text-pink-600">{p.count}</p></div>
                ))}</div>
                <button onClick={exportCSV} className="mb-6 px-4 py-2 bg-pink-600 text-white rounded">Export CSV</button>
                <button onClick={exportPDF} className="mb-6 ml-2 px-4 py-2 bg-purple-600 text-white rounded" > Export PDF </button>
                <div className="h-[400px]">
                  <ResponsiveContainer width="100%" height="100%">
                    <PieChart><Pie data={planSummary} dataKey="count" nameKey="plan" outerRadius={120} label>{planSummary.map((_,i)=><Cell key={i} fill={COLORS[i%COLORS.length]}/>)}</Pie><Tooltip/><Legend/></PieChart>
                  </ResponsiveContainer>
                </div>
              </>
            )}
          </section>
        )}

        {/* Reports */}
        {activeTab==="reports" && (
          <section>
            <h2 className="text-2xl font-bold text-pink-600 mb-6">Reported Reviews</h2>
            {reports.length===0?<p>No reports yet.</p>:(
              <div className="space-y-4">{reports.map(r=>(
                <div key={r.id} className="p-4 border rounded bg-gray-50">
                  <p><strong>Reported by:</strong> {r.reportedBy}</p>
                  {r.reason && <p><strong>Reason:</strong> {r.reason}</p>}
                  <p><strong>At:</strong> {r.createdAt?.toDate?r.createdAt.toDate().toLocaleString():new Date(r.createdAt).toLocaleString()}</p>
                  {r.reviewData?(<div className="mt-2 p-2 border rounded bg-white"><p><strong>Review by:</strong> {r.reviewData.userName}</p><p>{r.reviewData.content}</p><p>{r.reviewData.title} ({r.reviewData.type})</p></div>):<p className="text-red-500 mt-2">Original review deleted</p>}
                  <div className="flex gap-2 mt-3">
                    <button onClick={()=>handleDeleteReport(r.id)} className="px-3 py-1 bg-red-500 text-white rounded">Delete Report</button>
                    {r.reviewData && <button
                      onClick={async () => {
                        if (!confirm("Are you sure you want to delete this review and its report? This action cannot be undone.")) {
                          return;
                        }
                        await handleDeleteReviewAndReport(r);
                      }}
                      className="px-3 py-1 bg-red-700 text-white rounded"
                    >
                      Delete Review & Report
                    </button>
                    }
                  </div>
                </div>
              ))}</div>
            )}
          </section>
        )}

        {/* Reported Content */}
        {activeTab==="reportedContent" && (
          <section>
            <h2 className="text-2xl font-bold text-pink-600 mb-6">Reported Content</h2>
            {reportedContent.length===0?<p>No reported content.</p>:(
              <div className="space-y-4">{reportedContent.map(r=>(
                <div key={r.id} className="p-4 border rounded bg-gray-50">
                  <p><strong>Reported by:</strong> {r.reportedBy}</p>
                  {r.reason && <p><strong>Reason:</strong> {r.reason}</p>}
                  <p><strong>At:</strong> {r.createdAt?.toDate?r.createdAt.toDate().toLocaleString():new Date(r.createdAt).toLocaleString()}</p>
                  <p><strong>Item:</strong> {r.title} ({r.type})</p>
                  <button onClick={()=>handleDeleteReport(r.id,"reports-contents")} className="mt-2 px-3 py-1 bg-red-500 text-white rounded">Delete Report</button>
                </div>
              ))}</div>
            )}
          </section>
        )}

        {/* Analytics */}
{activeTab === "analytics" && (
  <section>
    <h2 className="text-2xl font-bold text-pink-600 mb-6">Analytics Dashboard</h2>
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
)}


      </section>
    </main>
  );
}
