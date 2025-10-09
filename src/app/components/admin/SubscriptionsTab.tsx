'use client';

import { useState, useMemo } from 'react';
import {
  PieChart,
  Pie,
  Cell,
  Tooltip,
  Legend,
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  LineChart,
  Line
} from "recharts";
import jsPDF from "jspdf";
import autoTable from "jspdf-autotable";
import {
  TrendingUp,
  TrendingDown,
  Users,
  DollarSign,
  Activity,
  Download,
  FileText,
  Filter,
  Calendar,
  CreditCard,
  Crown,
  Zap,
  Shield
} from 'lucide-react';

const COLORS = ["#ec4899", "#8b5cf6", "#3b82f6", "#10b981", "#f59e0b", "#ef4444"];

interface PlanSummary {
  plan: string;
  count: number;
  [key: string]: string | number;
}

interface PlanData {
  id: string;
  name: string;
  price: number;
  description: string;
  features: string[];
  billingPeriod?: string;
}

interface Subscription {
  uid?: string;
  fullName?: string;
  email?: string;
  plan?: string;
  startDate?: any;
  status?: string;
}

interface SubscriptionsTabProps {
  subscriptions: Subscription[];
  planSummary: PlanSummary[];
  plans: PlanData[];
}

export default function SubscriptionsTab({ subscriptions, planSummary, plans }: SubscriptionsTabProps) {
  const [viewMode, setViewMode] = useState<'grid' | 'chart'>('grid');
  const [chartType, setChartType] = useState<'pie' | 'bar'>('pie');
  const [filterStatus, setFilterStatus] = useState<'all' | 'active' | 'canceled'>('all');

  // Calculate metrics
  const metrics = useMemo(() => {
    const filteredSubs = subscriptions.filter(sub => 
      filterStatus === 'all' || sub.status === filterStatus
    );

    const totalUsers = filteredSubs.length;
    const activeUsers = filteredSubs.filter(s => s.status !== 'canceled').length;
    const canceledUsers = filteredSubs.filter(s => s.status === 'canceled').length;

    // Calculate MRR (Monthly Recurring Revenue)
    const mrr = filteredSubs.reduce((total, sub) => {
      if (sub.status === 'canceled') return total;
      const plan = plans.find(p => p.name === sub.plan);
      if (plan) {
        // Convert yearly to monthly
        const monthlyPrice = plan.billingPeriod === 'yearly' ? plan.price / 12 : plan.price;
        return total + monthlyPrice;
      }
      return total;
    }, 0);

    // Calculate ARR (Annual Recurring Revenue)
    const arr = mrr * 12;

    // Calculate churn rate
    const churnRate = totalUsers > 0 ? ((canceledUsers / totalUsers) * 100).toFixed(1) : "0.0";

    // Calculate average revenue per user
    const arpu = activeUsers > 0 ? (mrr / activeUsers).toFixed(2) : "0.00";

    // Growth calculation (dummy data - you'd need historical data for real calculation)
    const growth = "+12.5";

    return {
      totalUsers,
      activeUsers,
      canceledUsers,
      mrr: mrr.toFixed(2),
      arr: arr.toFixed(2),
      churnRate,
      arpu,
      growth
    };
  }, [subscriptions, plans, filterStatus]);

  // Revenue by plan
  const revenueByPlan = useMemo(() => {
    return planSummary.map(ps => {
      const plan = plans.find(p => p.name === ps.plan);
      const price = plan ? plan.price : 0;
      const monthlyPrice = plan?.billingPeriod === 'yearly' ? price / 12 : price;
      return {
        plan: ps.plan,
        count: ps.count,
        revenue: (monthlyPrice * ps.count).toFixed(2)
      };
    });
  }, [planSummary, plans]);

  const exportCSV = () => {
    if (!planSummary.length) return;
    const header = ["Plan", "Active Users", "Monthly Revenue"];
    const rows = revenueByPlan.map(p => [p.plan, p.count, `$${p.revenue}`]);
    const csv = [header, ...rows].map(e => e.join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.setAttribute("download", `subscription_report_${new Date().toISOString().split('T')[0]}.csv`);
    link.click();
    URL.revokeObjectURL(url);
  };

  const exportPDF = () => {
    if (!subscriptions.length) return;

    const doc = new jsPDF();
    
    // Header
    doc.setFontSize(20);
    doc.setTextColor(236, 72, 153);
    doc.text("Subscription Analytics Report", 14, 20);
    
    doc.setFontSize(10);
    doc.setTextColor(100);
    doc.text(`Generated: ${new Date().toLocaleDateString()}`, 14, 28);

    // Metrics Summary
    doc.setFontSize(14);
    doc.setTextColor(0);
    doc.text("Key Metrics", 14, 40);
    
    doc.setFontSize(10);
    doc.text(`Total Users: ${metrics.totalUsers}`, 14, 48);
    doc.text(`Active Users: ${metrics.activeUsers}`, 14, 54);
    doc.text(`MRR: $${metrics.mrr}`, 14, 60);
    doc.text(`Churn Rate: ${metrics.churnRate}%`, 14, 66);

    // Subscriptions Table
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
      startY: 75,
      theme: 'grid',
      headStyles: { fillColor: [236, 72, 153] }
    });

    doc.save(`subscriptions_report_${new Date().toISOString().split('T')[0]}.pdf`);
  };

  const getPlanIcon = (planName: string) => {
    if (planName.toLowerCase().includes('premium') || planName.toLowerCase().includes('pro')) {
      return <Crown className="w-5 h-5" />;
    }
    if (planName.toLowerCase().includes('enterprise')) {
      return <Shield className="w-5 h-5" />;
    }
    return <Zap className="w-5 h-5" />;
  };

  const filteredSummary = useMemo(() => {
    if (filterStatus === 'all') return planSummary;
    
    const filtered = subscriptions.filter(s => 
      filterStatus === 'active' ? s.status !== 'canceled' : s.status === 'canceled'
    );
    
    const summary = filtered.reduce((acc: Record<string, number>, sub) => {
      const plan = sub.plan || 'Free Plan';
      acc[plan] = (acc[plan] || 0) + 1;
      return acc;
    }, {});
    
    return Object.entries(summary).map(([plan, count]) => ({ plan, count }));
  }, [planSummary, subscriptions, filterStatus]);

  return (
    <section className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-3xl font-bold text-gray-800">Subscription Analytics</h2>
          <p className="text-gray-600 mt-1">Track revenue, growth, and user engagement</p>
        </div>
        <div className="flex gap-2">
          <button
            onClick={exportCSV}
            className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors"
          >
            <FileText className="w-4 h-4" />
            CSV
          </button>
          <button
            onClick={exportPDF}
            className="flex items-center gap-2 px-4 py-2 bg-purple-600 text-white rounded-lg hover:bg-purple-700 transition-colors"
          >
            <Download className="w-4 h-4" />
            PDF
          </button>
        </div>
      </div>

      {/* Filters */}
      <div className="bg-white rounded-lg shadow p-4">
        <div className="flex items-center gap-4">
          <Filter className="w-5 h-5 text-gray-400" />
          <select
            value={filterStatus}
            onChange={(e) => setFilterStatus(e.target.value as any)}
            className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
          >
            <option value="all">All Subscriptions</option>
            <option value="active">Active Only</option>
            <option value="canceled">Canceled Only</option>
          </select>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="bg-white rounded-lg shadow p-6 border-l-4 border-blue-500">
          <div className="flex items-center justify-between mb-2">
            <p className="text-sm text-gray-600 font-medium">Total Users</p>
            <Users className="w-5 h-5 text-blue-500" />
          </div>
          <p className="text-3xl font-bold text-gray-800">{metrics.totalUsers}</p>
          <div className="flex items-center gap-1 mt-2 text-sm text-green-600">
            <TrendingUp className="w-4 h-4" />
            <span>{metrics.growth}%</span>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow p-6 border-l-4 border-green-500">
          <div className="flex items-center justify-between mb-2">
            <p className="text-sm text-gray-600 font-medium">MRR</p>
            <DollarSign className="w-5 h-5 text-green-500" />
          </div>
          <p className="text-3xl font-bold text-gray-800">${metrics.mrr}</p>
          <p className="text-xs text-gray-500 mt-2">ARR: ${metrics.arr}</p>
        </div>

        <div className="bg-white rounded-lg shadow p-6 border-l-4 border-purple-500">
          <div className="flex items-center justify-between mb-2">
            <p className="text-sm text-gray-600 font-medium">Active Plans</p>
            <Activity className="w-5 h-5 text-purple-500" />
          </div>
          <p className="text-3xl font-bold text-gray-800">{planSummary.length}</p>
          <p className="text-xs text-gray-500 mt-2">{metrics.activeUsers} active users</p>
        </div>

        <div className="bg-white rounded-lg shadow p-6 border-l-4 border-red-500">
          <div className="flex items-center justify-between mb-2">
            <p className="text-sm text-gray-600 font-medium">Churn Rate</p>
            <TrendingDown className="w-5 h-5 text-red-500" />
          </div>
          <p className="text-3xl font-bold text-gray-800">{metrics.churnRate}%</p>
          <p className="text-xs text-gray-500 mt-2">{metrics.canceledUsers} canceled</p>
        </div>
      </div>

      {/* Additional Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="bg-gradient-to-br from-pink-50 to-purple-50 rounded-lg shadow p-6">
          <div className="flex items-center gap-3 mb-2">
            <div className="p-2 bg-white rounded-lg">
              <CreditCard className="w-5 h-5 text-pink-600" />
            </div>
            <p className="text-sm text-gray-700 font-medium">Avg Revenue Per User</p>
          </div>
          <p className="text-2xl font-bold text-gray-800">${metrics.arpu}</p>
        </div>

        <div className="bg-gradient-to-br from-blue-50 to-cyan-50 rounded-lg shadow p-6">
          <div className="flex items-center gap-3 mb-2">
            <div className="p-2 bg-white rounded-lg">
              <Calendar className="w-5 h-5 text-blue-600" />
            </div>
            <p className="text-sm text-gray-700 font-medium">Active Subscriptions</p>
          </div>
          <p className="text-2xl font-bold text-gray-800">{metrics.activeUsers}</p>
        </div>

        <div className="bg-gradient-to-br from-green-50 to-emerald-50 rounded-lg shadow p-6">
          <div className="flex items-center gap-3 mb-2">
            <div className="p-2 bg-white rounded-lg">
              <TrendingUp className="w-5 h-5 text-green-600" />
            </div>
            <p className="text-sm text-gray-700 font-medium">Growth Rate</p>
          </div>
          <p className="text-2xl font-bold text-gray-800">+{metrics.growth}%</p>
        </div>
      </div>

      {planSummary.length === 0 ? (
        <div className="bg-white rounded-lg shadow p-12 text-center">
          <Activity className="w-16 h-16 text-gray-300 mx-auto mb-4" />
          <p className="text-gray-500 text-lg">No subscription data available</p>
          <p className="text-gray-400 text-sm mt-2">Users with active plans will appear here</p>
        </div>
      ) : (
        <>
          {/* View Toggle */}
          <div className="flex justify-between items-center bg-white rounded-lg shadow p-4">
            <div className="flex gap-2">
              <button
                onClick={() => setViewMode('grid')}
                className={`px-4 py-2 rounded-lg transition-colors ${
                  viewMode === 'grid' 
                    ? 'bg-pink-600 text-white' 
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                Grid View
              </button>
              <button
                onClick={() => setViewMode('chart')}
                className={`px-4 py-2 rounded-lg transition-colors ${
                  viewMode === 'chart' 
                    ? 'bg-pink-600 text-white' 
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                Chart View
              </button>
            </div>

            {viewMode === 'chart' && (
              <div className="flex gap-2">
                <button
                  onClick={() => setChartType('pie')}
                  className={`px-4 py-2 rounded-lg transition-colors ${
                    chartType === 'pie' 
                      ? 'bg-purple-600 text-white' 
                      : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                  }`}
                >
                  Pie Chart
                </button>
                <button
                  onClick={() => setChartType('bar')}
                  className={`px-4 py-2 rounded-lg transition-colors ${
                    chartType === 'bar' 
                      ? 'bg-purple-600 text-white' 
                      : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                  }`}
                >
                  Bar Chart
                </button>
              </div>
            )}
          </div>

          {/* Content Area */}
          {viewMode === 'grid' ? (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {filteredSummary.map((p, i) => {
                const revenue = revenueByPlan.find(r => r.plan === p.plan);
                return (
                  <div
                    key={i}
                    className="bg-white rounded-xl shadow-lg hover:shadow-xl transition-all p-6 border-t-4"
                    style={{ borderTopColor: COLORS[i % COLORS.length] }}
                  >
                    <div className="flex items-center gap-3 mb-4">
                      <div
                        className="p-3 rounded-lg"
                        style={{ 
                          backgroundColor: `${COLORS[i % COLORS.length]}20`,
                          color: COLORS[i % COLORS.length]
                        }}
                      >
                        {getPlanIcon(p.plan)}
                      </div>
                      <div>
                        <h3 className="font-bold text-gray-800">{p.plan}</h3>
                        <p className="text-sm text-gray-500">Subscription Plan</p>
                      </div>
                    </div>

                    <div className="space-y-3">
                      <div className="flex justify-between items-center">
                        <span className="text-sm text-gray-600">Active Users</span>
                        <span className="text-2xl font-bold text-gray-800">{p.count}</span>
                      </div>
                      <div className="flex justify-between items-center">
                        <span className="text-sm text-gray-600">Monthly Revenue</span>
                        <span className="text-xl font-bold text-green-600">
                          ${revenue?.revenue || '0.00'}
                        </span>
                      </div>
                      <div className="pt-3 border-t">
                        <div className="flex justify-between items-center text-sm">
                          <span className="text-gray-600">Market Share</span>
                          <span className="font-semibold">
                            {((p.count / metrics.totalUsers) * 100).toFixed(1)}%
                          </span>
                        </div>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          ) : (
            <div className="bg-white rounded-lg shadow p-6">
              <div className="h-[400px]">
                <ResponsiveContainer width="100%" height="100%">
                  {chartType === 'pie' ? (
                    <PieChart>
                      <Pie
                        data={filteredSummary}
                        dataKey="count"
                        nameKey="plan"
                        cx="50%"
                        cy="50%"
                        outerRadius={140}
                        label={(entry) => `${entry.plan}: ${entry.count}`}
                      >
                        {filteredSummary.map((_, i) => (
                          <Cell key={i} fill={COLORS[i % COLORS.length]} />
                        ))}
                      </Pie>
                      <Tooltip />
                      <Legend />
                    </PieChart>
                  ) : (
                    <BarChart data={filteredSummary}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="plan" />
                      <YAxis />
                      <Tooltip />
                      <Legend />
                      <Bar dataKey="count" fill="#ec4899" />
                    </BarChart>
                  )}
                </ResponsiveContainer>
              </div>
            </div>
          )}

          {/* Revenue Breakdown Table */}
          <div className="bg-white rounded-lg shadow overflow-hidden">
            <div className="px-6 py-4 bg-gradient-to-r from-pink-50 to-purple-50 border-b">
              <h3 className="text-lg font-bold text-gray-800">Revenue Breakdown</h3>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Plan
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Users
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Monthly Revenue
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Market Share
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {revenueByPlan.map((item, i) => (
                    <tr key={i} className="hover:bg-gray-50 transition-colors">
                      <td className="px-6 py-4">
                        <div className="flex items-center gap-2">
                          <div
                            className="w-3 h-3 rounded-full"
                            style={{ backgroundColor: COLORS[i % COLORS.length] }}
                          />
                          <span className="font-medium text-gray-800">{item.plan}</span>
                        </div>
                      </td>
                      <td className="px-6 py-4 text-gray-600">{item.count}</td>
                      <td className="px-6 py-4">
                        <span className="font-semibold text-green-600">${item.revenue}</span>
                      </td>
                      <td className="px-6 py-4">
                        <div className="flex items-center gap-2">
                          <div className="flex-1 bg-gray-200 rounded-full h-2">
                            <div
                              className="h-2 rounded-full"
                              style={{
                                width: `${(item.count / metrics.totalUsers) * 100}%`,
                                backgroundColor: COLORS[i % COLORS.length]
                              }}
                            />
                          </div>
                          <span className="text-sm font-medium text-gray-700">
                            {((item.count / metrics.totalUsers) * 100).toFixed(1)}%
                          </span>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </section>
  );
}