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
  const [includeFree, setIncludeFree] = useState(true);

  // Get filtered subscriptions based on all filters
  const filteredSubscriptions = useMemo(() => {
    return subscriptions.filter(sub => {
      // Filter by free plan
      if (!includeFree && (!sub.plan || sub.plan === 'Free Plan')) {
        return false;
      }
      
      // Filter by status
      if (filterStatus === 'active') {
        return sub.status !== 'canceled';
      }
      if (filterStatus === 'canceled') {
        return sub.status === 'canceled';
      }
      
      return true; // 'all'
    });
  }, [subscriptions, filterStatus, includeFree]);

  // Calculate plan summary from filtered subscriptions
  const filteredPlanSummary = useMemo(() => {
    const summary = filteredSubscriptions.reduce((acc: Record<string, number>, sub) => {
      const plan = sub.plan || 'Free Plan';
      acc[plan] = (acc[plan] || 0) + 1;
      return acc;
    }, {});
    
    return Object.entries(summary).map(([plan, count]) => ({ 
      plan, 
      count 
    }));
  }, [filteredSubscriptions]);

  // Calculate metrics
  const metrics = useMemo(() => {
    const totalUsers = filteredSubscriptions.length;
    const activeUsers = filteredSubscriptions.filter(s => s.status !== 'canceled').length;
    const canceledUsers = filteredSubscriptions.filter(s => s.status === 'canceled').length;

    // Calculate MRR (Monthly Recurring Revenue) - only for active subscriptions
    const mrr = filteredSubscriptions.reduce((total, sub) => {
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
  }, [filteredSubscriptions, plans]);

  // Revenue by plan - calculated from filtered data
  const revenueByPlan = useMemo(() => {
    return filteredPlanSummary.map(ps => {
      const plan = plans.find(p => p.name === ps.plan);
      const price = plan ? plan.price : 0;
      const monthlyPrice = plan?.billingPeriod === 'yearly' ? price / 12 : price;
      
      // Count only active subscriptions for this plan
      const activeCount = filteredSubscriptions.filter(sub => 
        sub.plan === ps.plan && 
        sub.status !== 'canceled'
      ).length;
      
      return {
        plan: ps.plan,
        count: ps.count,
        activeCount,
        revenue: (monthlyPrice * activeCount).toFixed(2)
      };
    });
  }, [filteredPlanSummary, plans, filteredSubscriptions]);

  const exportPDF = () => {
    if (!filteredSubscriptions.length) return;

    const doc = new jsPDF();
    
    // Header
    doc.setFontSize(20);
    doc.setTextColor(236, 72, 153);
    doc.text("Subscription Analytics Report", 14, 20);
    
    doc.setFontSize(10);
    doc.setTextColor(100);
    doc.text(`Generated: ${new Date().toLocaleDateString()}`, 14, 28);
    doc.text(`Filter: ${filterStatus} | Include Free: ${includeFree ? 'Yes' : 'No'}`, 14, 33);

    // Metrics Summary
    doc.setFontSize(14);
    doc.setTextColor(0);
    doc.text("Key Metrics", 14, 43);
    
    doc.setFontSize(10);
    doc.text(`Total Users: ${metrics.totalUsers}`, 14, 51);
    doc.text(`Active Users: ${metrics.activeUsers}`, 14, 57);
    doc.text(`MRR: $${metrics.mrr}`, 14, 63);
    doc.text(`ARR: $${metrics.arr}`, 14, 69);
    doc.text(`Churn Rate: ${metrics.churnRate}%`, 14, 75);
    doc.text(`ARPU: $${metrics.arpu}`, 14, 81);

    // Subscriptions Table
    const tableColumn = ["Name", "Email", "Plan", "Start Date", "Status"];
    const tableRows: any[] = [];

    filteredSubscriptions.forEach((sub) => {
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
      startY: 90,
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
            onClick={exportPDF}
            disabled={filteredSubscriptions.length === 0}
            className="flex items-center gap-2 px-4 py-2 bg-purple-600 text-white rounded-lg hover:bg-purple-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          >
            <Download className="w-4 h-4" />
            Export as PDF
          </button>
        </div>
      </div>

      {/* Filters */}
      <div className="bg-white rounded-lg shadow p-4">
        <div className="flex items-center gap-4 flex-wrap">
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

          <label className="flex items-center gap-2 cursor-pointer">
            <input
              type="checkbox"
              checked={includeFree}
              onChange={(e) => setIncludeFree(e.target.checked)}
              className="w-4 h-4 text-pink-600 border-gray-300 rounded focus:ring-pink-500"
            />
            <span className="text-sm text-gray-700">Include Free Plan</span>
          </label>

          <div className="ml-auto text-sm text-gray-600">
            Showing {filteredSubscriptions.length} of {subscriptions.length} subscriptions
          </div>
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
            <p className="text-sm text-gray-600 font-medium">Active Users</p>
            <Activity className="w-5 h-5 text-purple-500" />
          </div>
          <p className="text-3xl font-bold text-gray-800">{metrics.activeUsers}</p>
          <p className="text-xs text-gray-500 mt-2">{filteredPlanSummary.length} active plans</p>
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
          <p className="text-xs text-gray-500 mt-1">Per active subscription</p>
        </div>

        <div className="bg-gradient-to-br from-blue-50 to-cyan-50 rounded-lg shadow p-6">
          <div className="flex items-center gap-3 mb-2">
            <div className="p-2 bg-white rounded-lg">
              <Calendar className="w-5 h-5 text-blue-600" />
            </div>
            <p className="text-sm text-gray-700 font-medium">Active Subscriptions</p>
          </div>
          <p className="text-2xl font-bold text-gray-800">{metrics.activeUsers}</p>
          <p className="text-xs text-gray-500 mt-1">Currently paying</p>
        </div>

        <div className="bg-gradient-to-br from-green-50 to-emerald-50 rounded-lg shadow p-6">
          <div className="flex items-center gap-3 mb-2">
            <div className="p-2 bg-white rounded-lg">
              <TrendingUp className="w-5 h-5 text-green-600" />
            </div>
            <p className="text-sm text-gray-700 font-medium">Growth Rate</p>
          </div>
          <p className="text-2xl font-bold text-gray-800">+{metrics.growth}%</p>
          <p className="text-xs text-gray-500 mt-1">Month over month</p>
        </div>
      </div>

      {filteredPlanSummary.length === 0 ? (
        <div className="bg-white rounded-lg shadow p-12 text-center">
          <Activity className="w-16 h-16 text-gray-300 mx-auto mb-4" />
          <p className="text-gray-500 text-lg">No subscription data available</p>
          <p className="text-gray-400 text-sm mt-2">
            {filterStatus !== 'all' && 'Try changing your filters or '}
            Users with active plans will appear here
          </p>
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
              {filteredPlanSummary.map((p, i) => {
                const revenue = revenueByPlan.find(r => r.plan === p.plan);
                const planDetails = plans.find(plan => plan.name === p.plan);
                
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
                        <p className="text-sm text-gray-500">
                          {planDetails?.billingPeriod || 'monthly'} plan
                        </p>
                      </div>
                    </div>

                    <div className="space-y-3">
                      <div className="flex justify-between items-center">
                        <span className="text-sm text-gray-600">Total Users</span>
                        <span className="text-2xl font-bold text-gray-800">{p.count}</span>
                      </div>
                      <div className="flex justify-between items-center">
                        <span className="text-sm text-gray-600">Active Users</span>
                        <span className="text-lg font-semibold text-blue-600">
                          {revenue?.activeCount || 0}
                        </span>
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
                            {metrics.totalUsers > 0 
                              ? ((p.count / metrics.totalUsers) * 100).toFixed(1)
                              : '0.0'}%
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
                        data={filteredPlanSummary}
                        dataKey="count"
                        nameKey="plan"
                        cx="50%"
                        cy="50%"
                        outerRadius={140}
                        label={(entry) => `${entry.plan}: ${entry.count}`}
                      >
                        {filteredPlanSummary.map((_, i) => (
                          <Cell key={i} fill={COLORS[i % COLORS.length]} />
                        ))}
                      </Pie>
                      <Tooltip />
                      <Legend />
                    </PieChart>
                  ) : (
                    <BarChart data={filteredPlanSummary}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="plan" />
                      <YAxis />
                      <Tooltip />
                      <Legend />
                      <Bar dataKey="count" fill="#ec4899" name="Subscribers" />
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
              <p className="text-sm text-gray-600 mt-1">Detailed plan performance metrics</p>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Plan
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Total Users
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Active Users
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
                        <span className="font-medium text-blue-600">{item.activeCount}</span>
                      </td>
                      <td className="px-6 py-4">
                        <span className="font-semibold text-green-600">${item.revenue}</span>
                      </td>
                      <td className="px-6 py-4">
                        <div className="flex items-center gap-2">
                          <div className="flex-1 bg-gray-200 rounded-full h-2">
                            <div
                              className="h-2 rounded-full transition-all"
                              style={{
                                width: `${metrics.totalUsers > 0 ? (item.count / metrics.totalUsers) * 100 : 0}%`,
                                backgroundColor: COLORS[i % COLORS.length]
                              }}
                            />
                          </div>
                          <span className="text-sm font-medium text-gray-700 min-w-[45px]">
                            {metrics.totalUsers > 0 
                              ? ((item.count / metrics.totalUsers) * 100).toFixed(1)
                              : '0.0'}%
                          </span>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
                <tfoot className="bg-gray-50 font-semibold">
                  <tr>
                    <td className="px-6 py-4 text-gray-800">Total</td>
                    <td className="px-6 py-4 text-gray-800">{metrics.totalUsers}</td>
                    <td className="px-6 py-4 text-blue-600">{metrics.activeUsers}</td>
                    <td className="px-6 py-4 text-green-600">
                      ${metrics.mrr}
                    </td>
                    <td className="px-6 py-4 text-gray-800">100%</td>
                  </tr>
                </tfoot>
              </table>
            </div>
          </div>
        </>
      )}
    </section>
  );
}