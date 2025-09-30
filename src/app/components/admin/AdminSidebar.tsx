'use client';

interface AdminSidebarProps {
  activeTab: string;
  setActiveTab: (tab: string) => void;
}

const TABS = [
  { id: "users", label: "Users" },
  { id: "reviews", label: "Reviews" },
  { id: "plans", label: "Plans" },
  { id: "subscriptions", label: "Subscriptions" },
  { id: "reports", label: "Reports" },
  { id: "reportedContent", label: "Reported Content" },
  { id: "FAQs", label: "FAQs" },
  { id: "userQuestions", label: "User Questions" },
  { id: "analytics", label: "Analytics" }
];

export default function AdminSidebar({ activeTab, setActiveTab }: AdminSidebarProps) {
  return (
    <aside className="w-64 mr-8 border-r border-gray-200">
      <ul className="space-y-2">
        {TABS.map(tab => (
          <li
            key={tab.id}
            className={`cursor-pointer px-4 py-2 rounded ${
              activeTab === tab.id ? "bg-pink-100 font-semibold" : "hover:bg-gray-100"
            }`}
            onClick={() => setActiveTab(tab.id)}
          >
            {tab.label}
          </li>
        ))}
      </ul>
    </aside>
  );
}
