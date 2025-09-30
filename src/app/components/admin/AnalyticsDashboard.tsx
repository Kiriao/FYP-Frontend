import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, LineChart, Line, AreaChart, Area, PieChart, Pie, Cell } from 'recharts';
import { TrendingUp, Users, MessageSquare, AlertTriangle, DollarSign, Eye, Download } from 'lucide-react';
import { useEffect, useState } from 'react';
import jsPDF from 'jspdf';
import autoTable from 'jspdf-autotable';

// Props interface for the analytics component
interface AnalyticsProps {
  users: any[];
  reviews: any[];
  contacts: any[];
  plans: any[];
  reports: any[];
  reportedContent: any[];
  subscriptions: any[];
}

export default function AnalyticsDashboard({ 
  users, 
  reviews, 
  contacts, 
  plans, 
  reports, 
  reportedContent, 
  subscriptions 
}: AnalyticsProps) {
  const [analytics, setAnalytics] = useState({
    totalRevenue: 0,
    activeUsers: 0,
    avgChurnRate: 0,
    planDistribution: [] as any[],
    recentGrowth: [] as any[],
    engagementMetrics: [] as any[]
  });

  useEffect(() => {
    calculateAnalytics();
  }, [users, reviews, contacts, plans, reports, reportedContent, subscriptions]);

  const calculateAnalytics = () => {
    // Calculate total revenue (MRR)
    let totalRevenue = 0;
    const planCounts: { [key: string]: number } = {};
    
    users.forEach(user => {
      if (user.plan && user.plan !== 'Free Plans') {
        const plan = plans.find(p => p.name === user.plan);
        if (plan) {
          totalRevenue += plan.price;
        }
      }
      
      // Count plan distribution
      const planName = user.plan || 'Free Plans';
      planCounts[planName] = (planCounts[planName] || 0) + 1;
    });

    // Plan distribution for chart
    const planDistribution = Object.entries(planCounts).map(([plan, count], index) => ({
      plan,
      count,
      percentage: Math.round((count / users.length) * 100),
      color: ['#ef4444', '#3b82f6', '#22c55e', '#f59e0b'][index % 4]
    }));

    // Active users (users with role !== 'inactive')
    const activeUsers = users.filter(u => u.role !== 'inactive').length;

    // Calculate churn rate (inactive users / total users)
    const inactiveUsers = users.filter(u => u.role === 'inactive').length;
    const avgChurnRate = users.length > 0 ? (inactiveUsers / users.length * 100) : 0;

    // Engagement metrics
    const engagementMetrics = [
      { 
        metric: 'Total Reviews', 
        value: reviews.length, 
        change: '+12%', 
        color: 'text-green-600',
        icon: MessageSquare 
      },
      { 
        metric: 'Contact Messages', 
        value: contacts.length, 
        change: '+5%', 
        color: 'text-green-600',
        icon: MessageSquare 
      },
      { 
        metric: 'Pending Reports', 
        value: reports.length + reportedContent.length, 
        change: reports.length > 5 ? '+8%' : '-15%', 
        color: reports.length > 5 ? 'text-red-600' : 'text-green-600',
        icon: AlertTriangle 
      },
      { 
        metric: 'Active Users', 
        value: activeUsers, 
        change: '+8%', 
        color: 'text-green-600',
        icon: Users 
      }
    ];

    // Mock recent growth data (you could enhance this with timestamps)
    const recentGrowth = [
      { month: 'Jan', users: Math.floor(users.length * 0.7), reviews: Math.floor(reviews.length * 0.6) },
      { month: 'Feb', users: Math.floor(users.length * 0.8), reviews: Math.floor(reviews.length * 0.7) },
      { month: 'Mar', users: Math.floor(users.length * 0.85), reviews: Math.floor(reviews.length * 0.8) },
      { month: 'Apr', users: Math.floor(users.length * 0.9), reviews: Math.floor(reviews.length * 0.85) },
      { month: 'May', users: Math.floor(users.length * 0.95), reviews: Math.floor(reviews.length * 0.9) },
      { month: 'Current', users: users.length, reviews: reviews.length }
    ];

    setAnalytics({
      totalRevenue,
      activeUsers,
      avgChurnRate: Number(avgChurnRate.toFixed(1)),
      planDistribution,
      recentGrowth,
      engagementMetrics
    });
  };

  const COLORS = ["#ef4444", "#3b82f6", "#22c55e", "#f59e0b"]

  const exportAnalyticsPDF = () => {
    const doc = new jsPDF();
    let yPosition = 20;

    // Title
    doc.setFontSize(20);
    doc.setTextColor(219, 39, 119); // Pink color
    doc.text("Analytics Dashboard Report", 14, yPosition);
    yPosition += 10;

    // Date
    doc.setFontSize(10);
    doc.setTextColor(107, 114, 128); // Gray color
    doc.text(`Generated: ${new Date().toLocaleDateString()} at ${new Date().toLocaleTimeString()}`, 14, yPosition);
    yPosition += 20;

    // Key Metrics Section
    doc.setFontSize(16);
    doc.setTextColor(0, 0, 0);
    doc.text("Key Performance Indicators", 14, yPosition);
    yPosition += 10;

    const kpiData = [
      ["Metric", "Value", "Details"],
      ["Monthly Revenue", `${analytics.totalRevenue}`, `From ${users.filter(u => u.plan !== 'Free Plans').length} paid users`],
      ["Active Users", analytics.activeUsers.toString(), `of ${users.length} total users`],
      ["Inactive Rate", `${analytics.avgChurnRate}%`, `${users.filter(u => u.role === 'inactive').length} inactive users`],
      ["Total Reviews", reviews.length.toString(), `${reviews.filter(r => r.showOnHome).length} shown on home`]
    ];

    autoTable(doc, {
      head: [kpiData[0]],
      body: kpiData.slice(1),
      startY: yPosition,
      theme: 'striped',
      headStyles: { fillColor: [219, 39, 119] }
    });

    yPosition = (doc as any).lastAutoTable.finalY + 20;

    // Plan Distribution Section
    doc.setFontSize(16);
    doc.text("Subscription Plan Distribution", 14, yPosition);
    yPosition += 10;

    const planData = [
      ["Plan", "Users", "Percentage", "Revenue Impact"]
    ];

    analytics.planDistribution.forEach(plan => {
      const planDetails = plans.find(p => p.name === plan.plan);
      const revenue = planDetails ? planDetails.price * plan.count : 0;
      planData.push([
        plan.plan,
        plan.count.toString(),
        `${plan.percentage}%`,
        `${revenue}`
      ]);
    });

    autoTable(doc, {
      head: [planData[0]],
      body: planData.slice(1),
      startY: yPosition,
      theme: 'striped',
      headStyles: { fillColor: [59, 130, 246] }
    });

    yPosition = (doc as any).lastAutoTable.finalY + 20;

    // Platform Activity Section
    doc.setFontSize(16);
    doc.text("Platform Activity", 14, yPosition);
    yPosition += 10;

    const activityData = [
      ["Activity Type", "Count", "Status"]
    ];

    analytics.engagementMetrics.forEach(metric => {
      activityData.push([
        metric.metric,
        metric.value.toString(),
        metric.change
      ]);
    });

    autoTable(doc, {
      head: [activityData[0]],
      body: activityData.slice(1),
      startY: yPosition,
      theme: 'striped',
      headStyles: { fillColor: [34, 197, 94] }
    });

    yPosition = (doc as any).lastAutoTable.finalY + 20;

    // Content Moderation Section
    doc.setFontSize(16);
    doc.text("Content Moderation Overview", 14, yPosition);
    yPosition += 10;

    const moderationData = [
      ["Type", "Count", "Description"],
      ["Review Reports", reports.length.toString(), "Reports on user reviews"],
      ["Content Reports", reportedContent.length.toString(), "Reports on platform content"],
      ["Approved Reviews", reviews.filter(r => r.showOnHome).length.toString(), "Reviews shown on homepage"],
      ["Contact Messages", contacts.length.toString(), "User contact submissions"]
    ];

    autoTable(doc, {
      head: [moderationData[0]],
      body: moderationData.slice(1),
      startY: yPosition,
      theme: 'striped',
      headStyles: { fillColor: [245, 158, 11] }
    });

    yPosition = (doc as any).lastAutoTable.finalY + 20;

    // Growth Summary (if there's space)
    if (yPosition < 250) {
      doc.setFontSize(16);
      doc.text("Growth Summary", 14, yPosition);
      yPosition += 10;

      const growthData = [
        ["Period", "Users", "Reviews"]
      ];

      analytics.recentGrowth.forEach(period => {
        growthData.push([
          period.month,
          period.users.toString(),
          period.reviews.toString()
        ]);
      });

      autoTable(doc, {
        head: [growthData[0]],
        body: growthData.slice(1),
        startY: yPosition,
        theme: 'striped',
        headStyles: { fillColor: [168, 85, 247] }
      });
    }

    // Save the PDF
    doc.save(`analytics-report-${new Date().toISOString().split('T')[0]}.pdf`);
  };

  return (
    <div className="space-y-6">
      {/* Header with Export Button */}
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-2xl font-bold text-pink-600">Analytics Dashboard</h2>
          <p className="text-gray-600 mt-1">Comprehensive business insights and metrics</p>
        </div>
        <button
          onClick={exportAnalyticsPDF}
          className="flex items-center gap-2 px-4 py-2 bg-pink-600 text-white rounded-lg hover:bg-pink-700 transition-colors"
        >
          <Download className="h-4 w-4" />
          Export PDF Report
        </button>
      </div>
      {/* Key Metrics Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-white p-6 rounded-lg border shadow-sm">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-600">Monthly Revenue</p>
              <p className="text-2xl font-bold text-gray-900">${analytics.totalRevenue}</p>
              <p className="text-sm text-green-600 mt-1">From {users.filter(u => u.plan !== 'Free Plans').length} paid users</p>
            </div>
            <DollarSign className="h-8 w-8 text-green-600" />
          </div>
        </div>

        <div className="bg-white p-6 rounded-lg border shadow-sm">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-600">Active Users</p>
              <p className="text-2xl font-bold text-gray-900">{analytics.activeUsers}</p>
              <p className="text-sm text-gray-600 mt-1">of {users.length} total</p>
            </div>
            <Users className="h-8 w-8 text-blue-600" />
          </div>
        </div>

        <div className="bg-white p-6 rounded-lg border shadow-sm">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-600">Inactive Rate</p>
              <p className="text-2xl font-bold text-gray-900">{analytics.avgChurnRate}%</p>
              <p className="text-sm text-gray-600 mt-1">{users.filter(u => u.role === 'inactive').length} inactive users</p>
            </div>
            <TrendingUp className="h-8 w-8 text-orange-600" />
          </div>
        </div>

        <div className="bg-white p-6 rounded-lg border shadow-sm">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-gray-600">Total Reviews</p>
              <p className="text-2xl font-bold text-gray-900">{reviews.length}</p>
              <p className="text-sm text-green-600 mt-1">{reviews.filter(r => r.showOnHome).length} shown on home</p>
            </div>
            <MessageSquare className="h-8 w-8 text-purple-600" />
          </div>
        </div>
      </div>

      {/* Charts Row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Growth Trends */}
        <div className="bg-white p-6 rounded-lg border shadow-sm">
          <h3 className="text-lg font-semibold mb-4 flex items-center gap-2">
            <TrendingUp className="h-5 w-5 text-blue-600" />
            Growth Trends
          </h3>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={analytics.recentGrowth}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="month" />
                <YAxis />
                <Tooltip />
                <Legend />
                <Line type="monotone" dataKey="users" stroke="#3b82f6" strokeWidth={2} name="Total Users" />
                <Line type="monotone" dataKey="reviews" stroke="#22c55e" strokeWidth={2} name="Reviews" />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Plan Distribution Pie Chart */}
        <div className="bg-white p-6 rounded-lg border shadow-sm">
          <h3 className="text-lg font-semibold mb-4 flex items-center gap-2">
            <DollarSign className="h-5 w-5 text-green-600" />
            Subscription Distribution
          </h3>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie 
                  data={analytics.planDistribution} 
                  dataKey="count" 
                  nameKey="plan" 
                  cx="50%" 
                  cy="50%" 
                  outerRadius={80} 
                  label={({ plan, percentage }) => `${plan}: ${percentage}%`}
                >
                  {analytics.planDistribution.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* Plan Details and Engagement */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Plan Breakdown */}
        <div className="bg-white p-6 rounded-lg border shadow-sm">
          <h3 className="text-lg font-semibold mb-4">Plan Breakdown</h3>
          <div className="space-y-4">
            {analytics.planDistribution.map((plan, index) => (
              <div key={index} className="flex items-center justify-between">
                <div className="flex items-center gap-3">
                  <div 
                    className="w-4 h-4 rounded" 
                    style={{ backgroundColor: plan.color }}
                  ></div>
                  <span className="font-medium">{plan.plan}</span>
                </div>
                <div className="text-right">
                  <p className="font-semibold">{plan.count} users</p>
                  <p className="text-sm text-gray-600">{plan.percentage}%</p>
                </div>
              </div>
            ))}
          </div>
          <div className="mt-4 pt-4 border-t">
            <div className="flex justify-between text-sm text-gray-600">
              <span>Revenue Generating Users</span>
              <span className="font-semibold">{users.filter(u => u.plan !== 'Free Plans').length} users</span>
            </div>
          </div>
        </div>

        {/* Engagement Metrics */}
        <div className="bg-white p-6 rounded-lg border shadow-sm">
          <h3 className="text-lg font-semibold mb-4 flex items-center gap-2">
            <Eye className="h-5 w-5 text-purple-600" />
            Platform Activity
          </h3>
          <div className="space-y-4">
            {analytics.engagementMetrics.map((metric, index) => {
              const Icon = metric.icon;
              return (
                <div key={index} className="flex items-center justify-between p-3 bg-gray-50 rounded">
                  <div className="flex items-center gap-3">
                    <Icon className="h-5 w-5 text-gray-600" />
                    <div>
                      <p className="font-medium">{metric.metric}</p>
                      <p className="text-2xl font-bold">{metric.value}</p>
                    </div>
                  </div>
                  <div className={`text-sm font-medium ${metric.color}`}>
                    {metric.change}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </div>

      {/* Content Moderation Stats */}
      <div className="bg-white p-6 rounded-lg border shadow-sm">
        <h3 className="text-lg font-semibold mb-4 flex items-center gap-2">
          <AlertTriangle className="h-5 w-5 text-orange-600" />
          Content Moderation Overview
        </h3>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <div className="text-center p-4 bg-red-50 rounded">
            <p className="text-2xl font-bold text-red-600">{reports.length}</p>
            <p className="text-sm text-gray-600">Review Reports</p>
          </div>
          <div className="text-center p-4 bg-yellow-50 rounded">
            <p className="text-2xl font-bold text-yellow-600">{reportedContent.length}</p>
            <p className="text-sm text-gray-600">Content Reports</p>
          </div>
          <div className="text-center p-4 bg-green-50 rounded">
            <p className="text-2xl font-bold text-green-600">{reviews.filter(r => r.showOnHome).length}</p>
            <p className="text-sm text-gray-600">Approved for Home</p>
          </div>
          <div className="text-center p-4 bg-blue-50 rounded">
            <p className="text-2xl font-bold text-blue-600">{contacts.length}</p>
            <p className="text-sm text-gray-600">Contact Messages</p>
          </div>
        </div>
      </div>

      {/* Quick Actions */}
      <div className="bg-white p-6 rounded-lg border shadow-sm">
        <h3 className="text-lg font-semibold mb-4">Quick Insights</h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
          <div className="p-4 bg-blue-50 rounded">
            <h4 className="font-semibold text-blue-800">Revenue Health</h4>
            <p className="text-blue-600">
              ${analytics.totalRevenue}/month from {users.filter(u => u.plan !== 'Free Plans').length} paying users
            </p>
          </div>
          <div className="p-4 bg-green-50 rounded">
            <h4 className="font-semibold text-green-800">User Engagement</h4>
            <p className="text-green-600">
              {reviews.length} total reviews, {reviews.filter(r => r.showOnHome).length} featured
            </p>
          </div>
          <div className="p-4 bg-orange-50 rounded">
            <h4 className="font-semibold text-orange-800">Moderation Queue</h4>
            <p className="text-orange-600">
              {reports.length + reportedContent.length} items need attention
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}