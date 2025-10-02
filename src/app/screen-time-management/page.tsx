"use client";

import { useEffect, useState } from "react";
import { auth, db } from "@/lib/firebase";
import { onAuthStateChanged, User } from "firebase/auth";
import {
  collection,
  getDocs,
  doc,
  getDoc,
  setDoc,
  query,
  where,
  Timestamp,
} from "firebase/firestore";
import { Clock, Shield, BookOpen, Play, AlertCircle, CheckCircle, TrendingUp, Calendar, Award, Lock, Unlock, BarChart3, Filter } from "lucide-react";

interface ChildOrStudent {
  id: string;
  fullName: string;
  email: string;
}

interface ScreenTimeSettings {
  dailyLimit: number;
  videoLimit: number;
  bookLimit: number;
  bedtimeStart: string;
  bedtimeEnd: string;
  weekendExtension: number;
  enabled: boolean;
  contentFiltering?: 'strict' | 'moderate' | 'relaxed';
  allowedCategories?: string[];
  rewardSystem?: boolean;
}

interface UsageData {
  date: string;
  videoMinutes: number;
  bookMinutes: number;
  totalMinutes: number;
  lastActivity: Timestamp;
  categoriesAccessed?: string[];
}

interface ChildSettings extends ScreenTimeSettings {
  childId: string;
  childName: string;
  email?: string;
  currentUsage?: UsageData;
  weeklyStats?: { day: string; minutes: number }[];
  status: 'within-limits' | 'approaching-limit' | 'limit-exceeded' | 'bedtime';
  rewardPoints?: number;
}

const DEFAULT_SETTINGS: ScreenTimeSettings = {
  dailyLimit: 120,
  videoLimit: 60,
  bookLimit: 0,
  bedtimeStart: "21:00",
  bedtimeEnd: "07:00",
  weekendExtension: 30,
  enabled: true,
  contentFiltering: 'moderate',
  allowedCategories: ['Educational', 'Science', 'Stories', 'Music'],
  rewardSystem: true,
};

const CONTENT_CATEGORIES = [
  'Educational', 'Science', 'Math', 'Stories', 'Music', 
  'Art', 'History', 'Geography', 'Languages', 'Games'
];

export default function ParentalControlsPage() {
  const [user, setUser] = useState<User | null>(null);
  const [role, setRole] = useState<string | null>(null);
  const [children, setChildren] = useState<ChildOrStudent[]>([]);
  const [childSettings, setChildSettings] = useState<ChildSettings[]>([]);
  const [selectedChildId, setSelectedChildId] = useState<string>("");
  const [loading, setLoading] = useState(false);
  const [viewMode, setViewMode] = useState<'overview' | 'detailed'>('overview');
  const [filterStatus, setFilterStatus] = useState<string>('all');
  const [showWeeklyChart, setShowWeeklyChart] = useState(false);

  useEffect(() => {
    const unsub = onAuthStateChanged(auth, async (u) => {
      setUser(u);
      if (u) {
        const userDoc = await getDoc(doc(db, "users", u.uid));
        if (userDoc.exists()) {
          const data = userDoc.data();
          setRole(data.role);

          if (data.role === "parent") {
            await loadChildren(u.uid);
          } else if (data.role === "educator") {
            await loadStudents(u.uid);
          }
        }
      }
    });
    return () => unsub();
  }, []);

  const loadChildren = async (parentId: string) => {
    const q = query(collection(db, "users"), where("parentId", "==", parentId));
    const snap = await getDocs(q);
    const kids = snap.docs.map((d) => ({
      id: d.id,
      fullName: d.data().fullName || "Unnamed Child",
      email: d.data().email || "",
    }));
    setChildren(kids);
    
    await loadAllChildSettings(kids);
    if (kids.length > 0 && !selectedChildId) {
      setSelectedChildId(kids[0].id);
    }
  };

  const loadStudents = async (educatorId: string) => {
    const q = query(collection(db, "users"), where("educatorId", "==", educatorId));
    const snap = await getDocs(q);
    const students = snap.docs.map((d) => ({
      id: d.id,
      fullName: d.data().fullName || "Unnamed Student",
      email: d.data().email || "",
    }));
    setChildren(students);
    
    await loadAllChildSettings(students);
    if (students.length > 0 && !selectedChildId) {
      setSelectedChildId(students[0].id);
    }
  };

  const loadAllChildSettings = async (childrenList: ChildOrStudent[]) => {
    const settingsPromises = childrenList.map(async (child) => {
      const settings = await loadChildSettings(child.id);
      const usage = await getTodayUsage(child.id);
      const weeklyStats = await getWeeklyUsage(child.id);
      const rewardPoints = await getRewardPoints(child.id);
      const status = calculateStatus(settings, usage);
      
      return {
        dailyLimit: settings.dailyLimit,
        videoLimit: settings.videoLimit,
        bookLimit: settings.bookLimit,
        bedtimeStart: settings.bedtimeStart,
        bedtimeEnd: settings.bedtimeEnd,
        weekendExtension: settings.weekendExtension,
        enabled: settings.enabled,
        contentFiltering: settings.contentFiltering,
        allowedCategories: settings.allowedCategories,
        rewardSystem: settings.rewardSystem,
        childId: child.id,
        childName: child.fullName,
        email: child.email,
        currentUsage: usage,
        weeklyStats,
        status,
        rewardPoints,
      } as ChildSettings;
    });

    const allSettings = await Promise.all(settingsPromises);
    setChildSettings(allSettings);
  };

  const loadChildSettings = async (childId: string): Promise<ScreenTimeSettings> => {
    try {
      const settingsDoc = await getDoc(doc(db, "users", childId, "settings", "screenTime"));
      if (settingsDoc.exists()) {
        return { ...DEFAULT_SETTINGS, ...settingsDoc.data() };
      }
      return DEFAULT_SETTINGS;
    } catch (error) {
      console.error("Failed to load child settings:", error);
      return DEFAULT_SETTINGS;
    }
  };

  const getTodayUsage = async (childId: string): Promise<UsageData> => {
    const today = new Date().toISOString().split('T')[0];
    try {
      const usageDoc = await getDoc(doc(db, "users", childId, "usage", today));
      if (usageDoc.exists()) {
        return usageDoc.data() as UsageData;
      }
      return {
        date: today,
        videoMinutes: 0,
        bookMinutes: 0,
        totalMinutes: 0,
        lastActivity: Timestamp.now(),
        categoriesAccessed: [],
      };
    } catch (error) {
      console.error("Failed to load usage data:", error);
      return {
        date: today,
        videoMinutes: 0,
        bookMinutes: 0,
        totalMinutes: 0,
        lastActivity: Timestamp.now(),
        categoriesAccessed: [],
      };
    }
  };

  const getWeeklyUsage = async (childId: string): Promise<{ day: string; minutes: number }[]> => {
    const weekDays = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    const today = new Date();
    const weeklyData: { day: string; minutes: number }[] = [];

    try {
      for (let i = 6; i >= 0; i--) {
        const date = new Date(today);
        date.setDate(date.getDate() - i);
        const dateStr = date.toISOString().split('T')[0];
        const dayName = weekDays[date.getDay()];

        const usageDoc = await getDoc(doc(db, "users", childId, "usage", dateStr));
        const minutes = usageDoc.exists() ? (usageDoc.data().totalMinutes || 0) : 0;
        
        weeklyData.push({ day: dayName, minutes });
      }
    } catch (error) {
      console.error("Failed to load weekly usage:", error);
      // Return empty data if error
      return weekDays.map(day => ({ day, minutes: 0 }));
    }

    return weeklyData;
  };

  const getRewardPoints = async (childId: string): Promise<number> => {
    try {
      const rewardsDoc = await getDoc(doc(db, "users", childId, "settings", "rewards"));
      if (rewardsDoc.exists()) {
        return rewardsDoc.data().points || 0;
      }
      return 0;
    } catch (error) {
      console.error("Failed to load reward points:", error);
      return 0;
    }
  };

  const calculateStatus = (settings: ScreenTimeSettings, usage: UsageData): ChildSettings['status'] => {
    if (!settings.enabled) return 'within-limits';

    const now = new Date();
    const currentTime = now.getHours() * 60 + now.getMinutes();
    const bedtimeStart = parseTime(settings.bedtimeStart);
    const bedtimeEnd = parseTime(settings.bedtimeEnd);

    // Check bedtime (handle overnight bedtime)
    if (bedtimeStart > bedtimeEnd) {
      if (currentTime >= bedtimeStart || currentTime <= bedtimeEnd) {
        return 'bedtime';
      }
    } else {
      if (currentTime >= bedtimeStart && currentTime <= bedtimeEnd) {
        return 'bedtime';
      }
    }

    const isWeekend = now.getDay() === 0 || now.getDay() === 6;
    const effectiveDailyLimit = settings.dailyLimit + (isWeekend ? settings.weekendExtension : 0);

    if (usage.totalMinutes >= effectiveDailyLimit) {
      return 'limit-exceeded';
    } else if (usage.totalMinutes >= effectiveDailyLimit * 0.8) {
      return 'approaching-limit';
    }

    return 'within-limits';
  };

  const parseTime = (timeString: string): number => {
    const [hours, minutes] = timeString.split(':').map(Number);
    return hours * 60 + minutes;
  };

  const updateChildSettings = async (childId: string, newSettings: Partial<ScreenTimeSettings>) => {
    setLoading(true);
    try {
      const currentSettings = childSettings.find(c => c.childId === childId);
      if (!currentSettings) {
        throw new Error("Child settings not found");
      }
      
      const updatedSettings: ScreenTimeSettings = {
        dailyLimit: newSettings.dailyLimit ?? currentSettings.dailyLimit,
        videoLimit: newSettings.videoLimit ?? currentSettings.videoLimit,
        bookLimit: newSettings.bookLimit ?? currentSettings.bookLimit,
        bedtimeStart: newSettings.bedtimeStart ?? currentSettings.bedtimeStart,
        bedtimeEnd: newSettings.bedtimeEnd ?? currentSettings.bedtimeEnd,
        weekendExtension: newSettings.weekendExtension ?? currentSettings.weekendExtension,
        enabled: newSettings.enabled ?? currentSettings.enabled,
        contentFiltering: newSettings.contentFiltering ?? currentSettings.contentFiltering,
        allowedCategories: newSettings.allowedCategories ?? currentSettings.allowedCategories,
        rewardSystem: newSettings.rewardSystem ?? currentSettings.rewardSystem,
      };
      
      await setDoc(doc(db, "users", childId, "settings", "screenTime"), updatedSettings);
      
      // Update local state
      setChildSettings(prev => 
        prev.map(child => 
          child.childId === childId 
            ? { ...child, ...updatedSettings, status: calculateStatus(updatedSettings, child.currentUsage!) }
            : child
        )
      );
    } catch (error) {
      console.error("Failed to update settings:", error);
      alert("Failed to update settings. Please try again.");
    } finally {
      setLoading(false);
    }
  };

  const getStatusColor = (status: ChildSettings['status']) => {
    switch (status) {
      case 'within-limits': return 'text-green-600 bg-green-50 border-green-200';
      case 'approaching-limit': return 'text-yellow-600 bg-yellow-50 border-yellow-200';
      case 'limit-exceeded': return 'text-red-600 bg-red-50 border-red-200';
      case 'bedtime': return 'text-purple-600 bg-purple-50 border-purple-200';
      default: return 'text-gray-600 bg-gray-50 border-gray-200';
    }
  };

  const getStatusIcon = (status: ChildSettings['status']) => {
    switch (status) {
      case 'within-limits': return <CheckCircle className="w-4 h-4" />;
      case 'approaching-limit': return <AlertCircle className="w-4 h-4" />;
      case 'limit-exceeded': return <AlertCircle className="w-4 h-4" />;
      case 'bedtime': return <Clock className="w-4 h-4" />;
      default: return <Clock className="w-4 h-4" />;
    }
  };

  const getStatusText = (status: ChildSettings['status']) => {
    switch (status) {
      case 'within-limits': return 'Within Limits';
      case 'approaching-limit': return 'Approaching Limit';
      case 'limit-exceeded': return 'Limit Exceeded';
      case 'bedtime': return 'Bedtime Hours';
      default: return 'Unknown';
    }
  };

  const formatMinutes = (minutes: number): string => {
    const hours = Math.floor(minutes / 60);
    const mins = minutes % 60;
    if (hours > 0) {
      return `${hours}h ${mins}m`;
    }
    return `${mins}m`;
  };

  const filteredChildren = childSettings.filter(child => {
    if (filterStatus === 'all') return true;
    return child.status === filterStatus;
  });

  const selectedChild = childSettings.find(child => child.childId === selectedChildId);

  const averageUsage = childSettings.length > 0 
    ? childSettings.reduce((acc, child) => acc + (child.currentUsage?.totalMinutes || 0), 0) / childSettings.length 
    : 0;

  if (!user) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-blue-50 via-white to-purple-50 flex items-center justify-center">
        <div className="text-center">
          <Shield className="w-16 h-16 mx-auto mb-4 text-gray-300" />
          <p className="text-gray-600">Please log in to access parental controls.</p>
        </div>
      </div>
    );
  }

  if (role !== "parent" && role !== "educator") {
    return (
      <div className="min-h-screen bg-gradient-to-br from-blue-50 via-white to-purple-50 flex items-center justify-center">
        <div className="text-center">
          <Shield className="w-16 h-16 mx-auto mb-4 text-red-300" />
          <p className="text-gray-600">Access denied. Only parents and educators can access these controls.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 via-white to-purple-50">
      <div className="max-w-7xl mx-auto p-6">
        {/* Header */}
        <div className="mb-8">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-3">
              <div className="bg-blue-600 p-3 rounded-xl">
                <Shield className="w-8 h-8 text-white" />
              </div>
              <div>
                <h1 className="text-3xl font-bold text-gray-900">
                  {role === "parent" ? "Parental Controls" : "Student Screen Time"}
                </h1>
                <p className="text-gray-600">
                  {role === "parent" 
                    ? "Manage screen time and content for your children"
                    : "Monitor and manage student screen time"
                  }
                </p>
              </div>
            </div>
            
{/*             <div className="flex items-center gap-3">
              <button
                onClick={() => setViewMode(viewMode === 'overview' ? 'detailed' : 'overview')}
                className="px-4 py-2 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors font-medium"
              >
                {viewMode === 'overview' ? 'Detailed View' : 'Overview'}
              </button>
            </div> */}
          </div>

          {children.length === 0 ? (
            <div className="text-center py-12 bg-white rounded-xl border border-gray-200">
              <Shield className="w-16 h-16 mx-auto mb-4 text-gray-300" />
              <p className="text-gray-600">
                {role === "parent" 
                  ? "No children accounts found. Add children to your account to manage their screen time."
                  : "No student accounts found."
                }
              </p>
            </div>
          ) : (
            <>
              {/* Statistics Bar */}
              <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                <div className="bg-white rounded-xl p-4 border border-gray-200">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-sm text-gray-600">
                        {role === "parent" ? "Total Children" : "Total Students"}
                      </p>
                      <p className="text-2xl font-bold text-gray-900">{childSettings.length}</p>
                    </div>
                    <div className="bg-blue-100 p-3 rounded-lg">
                      <Shield className="w-6 h-6 text-blue-600" />
                    </div>
                  </div>
                </div>

                <div className="bg-white rounded-xl p-4 border border-gray-200">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-sm text-gray-600">Avg Daily Usage</p>
                      <p className="text-2xl font-bold text-gray-900">{formatMinutes(Math.round(averageUsage))}</p>
                    </div>
                    <div className="bg-green-100 p-3 rounded-lg">
                      <TrendingUp className="w-6 h-6 text-green-600" />
                    </div>
                  </div>
                </div>

                <div className="bg-white rounded-xl p-4 border border-gray-200">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-sm text-gray-600">Within Limits</p>
                      <p className="text-2xl font-bold text-green-600">
                        {childSettings.filter(c => c.status === 'within-limits').length}
                      </p>
                    </div>
                    <div className="bg-green-100 p-3 rounded-lg">
                      <CheckCircle className="w-6 h-6 text-green-600" />
                    </div>
                  </div>
                </div>

                <div className="bg-white rounded-xl p-4 border border-gray-200">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-sm text-gray-600">Need Attention</p>
                      <p className="text-2xl font-bold text-red-600">
                        {childSettings.filter(c => c.status === 'limit-exceeded').length}
                      </p>
                    </div>
                    <div className="bg-red-100 p-3 rounded-lg">
                      <AlertCircle className="w-6 h-6 text-red-600" />
                    </div>
                  </div>
                </div>
              </div>
            </>
          )}
        </div>

        {children.length > 0 && (
          <>
            {/* Filter Bar */}
            <div className="mb-6 flex items-center gap-4">
              <div className="flex items-center gap-2">
                <Filter className="w-5 h-5 text-gray-600" />
                <span className="text-sm font-medium text-gray-700">Filter:</span>
              </div>
              <div className="flex gap-2 flex-wrap">
                {['all', 'within-limits', 'approaching-limit', 'limit-exceeded'].map(status => (
                  <button
                    key={status}
                    onClick={() => setFilterStatus(status)}
                    className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                      filterStatus === status
                        ? 'bg-blue-600 text-white'
                        : 'bg-white text-gray-700 border border-gray-300 hover:bg-gray-50'
                    }`}
                  >
                    {status === 'all' ? 'All' : status.split('-').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ')}
                  </button>
                ))}
              </div>
            </div>

            {/* Children Cards Grid */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-8">
              {filteredChildren.map((child) => (
                <div 
                  key={child.childId} 
                  className={`bg-white rounded-xl p-6 border-2 transition-all cursor-pointer hover:shadow-lg ${
                    selectedChildId === child.childId ? 'border-blue-500 shadow-lg' : 'border-gray-200'
                  }`}
                  onClick={() => setSelectedChildId(child.childId)}
                >
                  <div className="flex items-center justify-between mb-4">
                    <div className="flex items-center gap-3">
                      <div className="bg-gradient-to-br from-blue-500 to-purple-500 w-12 h-12 rounded-full flex items-center justify-center text-white font-bold text-lg">
                        {child.childName.charAt(0)}
                      </div>
                      <div>
                        <h3 className="font-semibold text-gray-900">{child.childName}</h3>
                        <p className="text-xs text-gray-500">{child.email || `ID: ${child.childId}`}</p>
                      </div>
                    </div>
                  </div>
                  
                  <span className={`inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium border mb-4 ${getStatusColor(child.status)}`}>
                    {getStatusIcon(child.status)}
                    {getStatusText(child.status)}
                  </span>

                  {child.currentUsage && (
                    <div className="space-y-3">
                      <div className="flex justify-between items-center">
                        <span className="text-sm text-gray-600">Today's Usage</span>
                        <span className="text-lg font-bold text-gray-900">{formatMinutes(child.currentUsage.totalMinutes)}</span>
                      </div>
                      
                      <div className="space-y-2">
                        <div className="flex items-center justify-between text-sm">
                          <span className="flex items-center gap-1.5 text-gray-600">
                            <Play className="w-4 h-4 text-red-500" />
                            Videos
                          </span>
                          <span className="font-medium text-gray-500">{formatMinutes(child.currentUsage.videoMinutes)}</span>
                        </div>
                        <div className="flex items-center justify-between text-sm">
                          <span className="flex items-center gap-1.5 text-gray-600">
                            <BookOpen className="w-4 h-4 text-blue-500" />
                            Books
                          </span>
                          <span className="font-medium text-gray-500">{formatMinutes(child.currentUsage.bookMinutes)}</span>
                        </div>
                      </div>

                      {child.enabled && (
                        <div className="mt-4">
                          <div className="flex justify-between text-xs text-gray-500 mb-2">
                            <span>Daily Limit: {formatMinutes(child.dailyLimit)}</span>
                            <span>{Math.round((child.currentUsage.totalMinutes / child.dailyLimit) * 100)}%</span>
                          </div>
                          <div className="w-full bg-gray-200 rounded-full h-3 overflow-hidden">
                            <div 
                              className={`h-3 rounded-full transition-all duration-500 ${
                                child.status === 'limit-exceeded' ? 'bg-gradient-to-r from-red-500 to-red-600' :
                                child.status === 'approaching-limit' ? 'bg-gradient-to-r from-yellow-500 to-orange-500' : 
                                'bg-gradient-to-r from-green-500 to-emerald-500'
                              }`}
                              style={{ 
                                width: `${Math.min(100, (child.currentUsage.totalMinutes / child.dailyLimit) * 100)}%` 
                              }}
                            />
                          </div>
                        </div>
                      )}

                      {child.rewardSystem && child.rewardPoints !== undefined && (
                        <div className="mt-3 pt-3 border-t border-gray-200">
                          <div className="flex items-center justify-between">
                            <span className="flex items-center gap-1.5 text-sm text-gray-600">
                              <Award className="w-4 h-4 text-yellow-500" />
                              Reward Points
                            </span>
                            <span className="font-bold text-yellow-600">{child.rewardPoints}</span>
                          </div>
                        </div>
                      )}
                    </div>
                  )}
                </div>
              ))}
            </div>

            {/* Detailed Settings Panel */}
            {selectedChild && (
              <div className="bg-white rounded-xl border border-gray-200 p-6">
                <div className="flex items-center justify-between mb-6">
                  <h2 className="text-2xl font-bold text-gray-900">
                    Settings for {selectedChild.childName}
                  </h2>
                  <button
                    onClick={() => setShowWeeklyChart(!showWeeklyChart)}
                    className="flex items-center gap-2 px-4 py-2 bg-blue-50 text-blue-600 rounded-lg hover:bg-blue-100 transition-colors"
                  >
                    <BarChart3 className="w-4 h-4" />
                    {showWeeklyChart ? 'Hide' : 'Show'} Weekly Stats
                  </button>
                </div>

                {/* Weekly Chart */}
                {showWeeklyChart && selectedChild.weeklyStats && selectedChild.weeklyStats.length > 0 && (
                  <div className="mb-6 p-4 bg-gray-50 rounded-xl">
                    <h3 className="font-semibold text-gray-900 mb-4">Weekly Usage Pattern</h3>
                    <div className="flex items-end justify-between h-48 gap-2">
                      {selectedChild.weeklyStats.map((stat, index) => {
                        const maxMinutes = Math.max(...selectedChild.weeklyStats!.map(s => s.minutes), 1);
                        const heightPercent = (stat.minutes / maxMinutes) * 100;
                        return (
                          <div key={index} className="flex-1 flex flex-col items-center gap-2">
                            <div className="w-full bg-blue-500 rounded-t-lg transition-all hover:bg-blue-600 relative group" 
                                 style={{ height: `${Math.max(heightPercent, 2)}%` }}>
                              <span className="absolute -top-8 left-1/2 -translate-x-1/2 bg-gray-900 text-white text-xs px-2 py-1 rounded opacity-0 group-hover:opacity-100 transition-opacity whitespace-nowrap">
                                {formatMinutes(stat.minutes)}
                              </span>
                            </div>
                            <span className="text-xs font-medium text-gray-600">{stat.day}</span>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                )}
                
                <div className="space-y-6">
                  {/* Enable/Disable Toggle */}
                  <div className="flex items-center justify-between p-4 bg-gray-50 rounded-xl">
                    <div className="flex items-center gap-3">
                      {selectedChild.enabled ? <Lock className="w-5 h-5 text-blue-600" /> : <Unlock className="w-5 h-5 text-gray-400" />}
                      <div>
                        <h3 className="font-semibold text-gray-900">Screen Time Controls</h3>
                        <p className="text-sm text-gray-600">
                          {selectedChild.enabled ? 'Active - Limits are enforced' : 'Disabled - No restrictions'}
                        </p>
                      </div>
                    </div>
                    <label className="relative inline-flex items-center cursor-pointer">
                      <input
                        type="checkbox"
                        checked={selectedChild.enabled}
                        onChange={(e) => updateChildSettings(selectedChild.childId, { 
                          enabled: e.target.checked 
                        })}
                        className="sr-only peer"
                        disabled={loading}
                      />
                      <div className="w-14 h-7 bg-gray-300 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-300 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-0.5 after:left-[4px] after:bg-white after:rounded-full after:h-6 after:w-6 after:transition-all peer-checked:bg-blue-600"></div>
                    </label>
                  </div>

                  {selectedChild.enabled && (
                    <>
                      {/* Time Limits */}
                      <div className="space-y-4">
                        <h3 className="font-semibold text-gray-900 flex items-center gap-2">
                          <Clock className="w-5 h-5 text-blue-600" />
                          Daily Time Limits
                        </h3>
                        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                          <div>
                            <label className="block text-sm font-medium text-gray-700 mb-2">
                              Daily Limit
                            </label>
                            <div className="relative">
                              <input
                                type="number"
                                min="0"
                                max="1440"
                                step="15"
                                value={selectedChild.dailyLimit}
                                onChange={(e) => updateChildSettings(selectedChild.childId, { 
                                  dailyLimit: parseInt(e.target.value) || 0
                                })}
                                className="w-full px-4 py-2.5 border text-gray-900 border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 pr-16"
                                disabled={loading}
                              />
                              <span className="absolute right-3 top-1/2 -translate-y-1/2 text-sm text-gray-500">
                                minutes
                              </span>
                            </div>
                            <p className="text-xs text-gray-500 mt-1">{formatMinutes(selectedChild.dailyLimit)} total</p>
                          </div>
                          
                          <div>
                            <label className="block text-sm font-medium text-gray-700 mb-2">
                              Video Limit
                            </label>
                            <div className="relative">
                              <input
                                type="number"
                                min="0"
                                max="1440"
                                step="15"
                                value={selectedChild.videoLimit}
                                onChange={(e) => updateChildSettings(selectedChild.childId, { 
                                  videoLimit: parseInt(e.target.value) || 0
                                })}
                                className="w-full px-4 py-2.5 border text-gray-900 border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 pr-16"
                                disabled={loading}
                              />
                              <span className="absolute right-3 top-1/2 -translate-y-1/2 text-sm text-gray-500">
                                minutes
                              </span>
                            </div>
                            <p className="text-xs text-gray-500 mt-1">{formatMinutes(selectedChild.videoLimit)} for videos</p>
                          </div>
                          
                          <div>
                            <label className="block text-sm font-medium text-gray-700 mb-2">
                              Weekend Bonus
                            </label>
                            <div className="relative">
                              <input
                                type="number"
                                min="0"
                                max="240"
                                step="15"
                                value={selectedChild.weekendExtension}
                                onChange={(e) => updateChildSettings(selectedChild.childId, { 
                                  weekendExtension: parseInt(e.target.value) || 0
                                })}
                                className="w-full px-4 py-2.5 border text-gray-900 border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 pr-16"
                                disabled={loading}
                              />
                              <span className="absolute right-3 top-1/2 -translate-y-1/2 text-sm text-gray-500">
                                minutes
                              </span>
                            </div>
                            <p className="text-xs text-gray-500 mt-1">Extra on Sat & Sun</p>
                          </div>
                        </div>
                      </div>

                      {/* Bedtime Settings */}
                      <div className="space-y-4">
                        <h3 className="font-semibold text-gray-900 flex items-center gap-2">
                          <Clock className="w-5 h-5 text-purple-600" />
                          Bedtime Schedule
                        </h3>
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                          <div>
                            <label className="block text-sm font-medium text-gray-700 mb-2">
                              Bedtime Start
                            </label>
                            <input
                              type="time"
                              value={selectedChild.bedtimeStart}
                              onChange={(e) => updateChildSettings(selectedChild.childId, { 
                                bedtimeStart: e.target.value
                              })}
                              className="w-full px-4 py-2.5 border text-gray-900 border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                              disabled={loading}
                            />
                          </div>
                          
                          <div>
                            <label className="block text-sm font-medium text-gray-700 mb-2">
                              Bedtime End
                            </label>
                            <input
                              type="time"
                              value={selectedChild.bedtimeEnd}
                              onChange={(e) => updateChildSettings(selectedChild.childId, { 
                                bedtimeEnd: e.target.value
                              })}
                              className="w-full px-4 py-2.5 border border-gray-300 text-gray-900 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                              disabled={loading}
                            />
                          </div>
                        </div>
                        <div className="bg-purple-50 border border-purple-200 rounded-lg p-4">
                          <p className="text-sm text-purple-800">
                            All content will be blocked during bedtime hours ({selectedChild.bedtimeStart} - {selectedChild.bedtimeEnd})
                          </p>
                        </div>
                      </div>

                      {/* Content Filtering */}
                      <div className="space-y-4">
                        <h3 className="font-semibold text-gray-900 flex items-center gap-2">
                          <Shield className="w-5 h-5 text-green-600" />
                          Content Filtering
                        </h3>
                        <div>
                          <label className="block text-sm font-medium text-gray-700 mb-2">
                            Filter Level
                          </label>
                          <div className="grid grid-cols-3 gap-3">
                            {(['strict', 'moderate', 'relaxed'] as const).map((level) => (
                              <button
                                key={level}
                                onClick={() => updateChildSettings(selectedChild.childId, { 
                                  contentFiltering: level 
                                })}
                                className={`px-4 py-3 rounded-lg border-2 font-medium transition-all ${
                                  selectedChild.contentFiltering === level
                                    ? 'border-blue-500 bg-blue-50 text-blue-700'
                                    : 'border-gray-300 bg-white text-gray-700 hover:border-gray-400'
                                }`}
                                disabled={loading}
                              >
                                {level.charAt(0).toUpperCase() + level.slice(1)}
                              </button>
                            ))}
                          </div>
                        </div>

                        <div>
                          <label className="block text-sm font-medium text-gray-700 mb-3">
                            Allowed Categories
                          </label>
                          <div className="grid grid-cols-2 md:grid-cols-5 gap-2">
                            {CONTENT_CATEGORIES.map((category) => (
                              <button
                                key={category}
                                onClick={() => {
                                  const currentCategories = selectedChild.allowedCategories || [];
                                  const newCategories = currentCategories.includes(category)
                                    ? currentCategories.filter(c => c !== category)
                                    : [...currentCategories, category];
                                  updateChildSettings(selectedChild.childId, { 
                                    allowedCategories: newCategories 
                                  });
                                }}
                                className={`px-3 py-2 rounded-lg border text-sm font-medium transition-all ${
                                  (selectedChild.allowedCategories || []).includes(category)
                                    ? 'border-green-500 bg-green-50 text-green-700'
                                    : 'border-gray-300 bg-white text-gray-600 hover:border-gray-400'
                                }`}
                                disabled={loading}
                              >
                                {category}
                              </button>
                            ))}
                          </div>
                        </div>
                      </div>

                      {/* Reward System */}
                      <div className="space-y-4">
                        <div className="flex items-center justify-between p-4 bg-gradient-to-r from-yellow-50 to-orange-50 rounded-xl border border-yellow-200">
                          <div className="flex items-center gap-3">
                            <Award className="w-6 h-6 text-yellow-600" />
                            <div>
                              <h3 className="font-semibold text-gray-900">Reward System</h3>
                              <p className="text-sm text-gray-600">Earn points for reading books and staying within limits</p>
                            </div>
                          </div>
                          <label className="relative inline-flex items-center cursor-pointer">
                            <input
                              type="checkbox"
                              checked={selectedChild.rewardSystem || false}
                              onChange={(e) => updateChildSettings(selectedChild.childId, { 
                                rewardSystem: e.target.checked 
                              })}
                              className="sr-only peer"
                              disabled={loading}
                            />
                            <div className="w-14 h-7 bg-gray-300 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-yellow-300 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-0.5 after:left-[4px] after:bg-white after:rounded-full after:h-6 after:w-6 after:transition-all peer-checked:bg-yellow-500"></div>
                          </label>
                        </div>
                      </div>

                      {/* Reading Encouragement */}
                      <div className="bg-gradient-to-r from-blue-50 to-indigo-50 border border-blue-200 rounded-xl p-6">
                        <div className="flex items-start gap-4">
                          <div className="bg-blue-600 p-3 rounded-lg">
                            <BookOpen className="w-6 h-6 text-white" />
                          </div>
                          <div>
                            <h4 className="font-semibold text-blue-900 mb-2">Reading Encouragement Active</h4>
                            <p className="text-sm text-blue-800">
                              Books have unlimited time by default to encourage reading. Video limits help balance screen time while promoting educational content. Children earn bonus reward points for reading!
                            </p>
                          </div>
                        </div>
                      </div>
                    </>
                  )}
                </div>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}