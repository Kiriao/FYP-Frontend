'use client';

import { 
  LayoutDashboard, 
  Users, 
  MessageSquare, 
  CreditCard, 
  UserCheck, 
  AlertTriangle, 
  Flag, 
  HelpCircle, 
  MessageCircle, 
  BarChart3,
  Ticket
} from 'lucide-react';

interface AdminSidebarProps {
  activeTab: string;
  setActiveTab: (tab: string) => void;
}

const TABS = [
  { id: "overview", label: "Overview", icon: LayoutDashboard },
  { id: "users", label: "Users", icon: Users },
  { id: "reviews", label: "Reviews", icon: MessageSquare },
  { id: "plans", label: "Plans", icon: CreditCard },
  { id: "subscriptions", label: "Subscriptions", icon: UserCheck },
  { id: "promoCodes", label: "Promo Codes", icon: Ticket },
  { id: "reports", label: "Reports", icon: AlertTriangle },
  { id: "reportedContent", label: "Reported Content", icon: Flag },
  { id: "FAQs", label: "FAQs", icon: HelpCircle },
  { id: "userQuestions", label: "User Questions", icon: MessageCircle },
  { id: "analytics", label: "Analytics", icon: BarChart3 }
];

export default function AdminSidebar({ activeTab, setActiveTab }: AdminSidebarProps) {
  return (
    <aside className="w-72 bg-white rounded-xl shadow-lg p-6 h-fit sticky top-6">
      <div className="mb-8">
        <h2 className="text-2xl font-bold text-gray-800 mb-2">Admin Panel</h2>
        <p className="text-sm text-gray-500">Manage your platform</p>
      </div>
      
      <nav>
        <ul className="space-y-2">
          {TABS.map(tab => {
            const Icon = tab.icon;
            const isActive = activeTab === tab.id;
            
            return (
              <li key={tab.id}>
                <button
                  onClick={() => setActiveTab(tab.id)}
                  className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg transition-all duration-200 ${
                    isActive 
                      ? "bg-gradient-to-r from-pink-600 to-purple-600 text-white shadow-md transform scale-105" 
                      : "text-gray-700 hover:bg-gray-100 hover:translate-x-1"
                  }`}
                >
                  <Icon className={`w-5 h-5 ${isActive ? 'text-white' : 'text-gray-600'}`} />
                  <span className="font-medium">{tab.label}</span>
                </button>
              </li>
            );
          })}
        </ul>
      </nav>

      <div className="mt-8 pt-6 border-t border-gray-200">
        <div className="bg-gradient-to-br from-pink-50 to-purple-50 rounded-lg p-4">
          <p className="text-xs font-semibold text-gray-700 mb-1">Quick Tip</p>
          <p className="text-xs text-gray-600">
            Use the Overview tab to get a quick snapshot of your platform's performance.
          </p>
        </div>
      </div>
    </aside>
  );
}