'use client';

import { MdSubscriptions } from "react-icons/md";
import {
  PieChart,
  Pie,
  Cell,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import jsPDF from "jspdf";
import autoTable from "jspdf-autotable";

const COLORS = ["#ef4444", "#3b82f6", "#22c55e", "#f59e0b", "#f02d88ff"];

interface PlanSummary {
  plan: string;
  count: number;
}

interface PlanData {
  id: string;
  name: string;
  price: number;
  description: string;
  features: string[];
}

interface SubscriptionsTabProps {
  subscriptions: any[];
  planSummary: PlanSummary[];
  plans: PlanData[];
}

export default function SubscriptionsTab({ subscriptions, planSummary, plans }: SubscriptionsTabProps) {
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

  const exportCSV = () => {
    if (!planSummary.length) return;
    const header = ["Plan", "Active Users"];
    const rows = planSummary.map(p => [p.plan, p.count]);
    const csv = [header, ...rows].map(e => e.join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.setAttribute("download", "subscription_report.csv");
    link.click();
    URL.revokeObjectURL(url);
  };

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

  return (
    <section>
      <h2 className="text-2xl font-bold text-pink-600 mb-6 flex items-center gap-2">
        <MdSubscriptions /> Subscriptions
      </h2>
      
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

      {planSummary.length === 0 ? (
        <p>No subscriptions yet.</p>
      ) : (
        <>
          <div className="grid md:grid-cols-3 gap-6 mb-12">
            {planSummary.map((p, i) => (
              <div key={i} className="p-6 bg-white border rounded shadow text-center">
                <h3 className="font-bold">{p.plan}</h3>
                <p className="text-2xl text-pink-600">{p.count}</p>
              </div>
            ))}
          </div>
          <button
            onClick={exportCSV}
            className="mb-6 px-4 py-2 bg-pink-600 text-white rounded"
          >
            Export CSV
          </button>
          <button
            onClick={exportPDF}
            className="mb-6 ml-2 px-4 py-2 bg-purple-600 text-white rounded"
          >
            Export PDF
          </button>
          <div className="h-[400px]">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={planSummary}
                  dataKey="count"
                  nameKey="plan"
                  outerRadius={120}
                  label
                >
                  {planSummary.map((_, i) => (
                    <Cell key={i} fill={COLORS[i % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip />
                <Legend />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </>
      )}
    </section>
  );
}