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
import { Clock, Shield, BookOpen, Play, AlertCircle, CheckCircle } from "lucide-react";

interface ChildOrStudent {
  id: string;
  fullName: string;
  email: string;
}

interface ScreenTimeSettings {
  dailyLimit: number; // minutes per day
  videoLimit: number; // minutes for videos per day
  bookLimit: number; // minutes for books per day (0 = unlimited)
  bedtimeStart: string; // "22:00" format
  bedtimeEnd: string; // "07:00" format
  weekendExtension: number; // extra minutes on weekends
  enabled: boolean;
}

interface UsageData {
  date: string; // YYYY-MM-DD format
  videoMinutes: number;
  bookMinutes: number;
  totalMinutes: number;
  lastActivity: Timestamp;
}

interface ChildSettings extends ScreenTimeSettings {
  childId: string;
  childName: string;
  currentUsage?: UsageData;
  status: 'within-limits' | 'approaching-limit' | 'limit-exceeded' | 'bedtime';
}

const DEFAULT_SETTINGS: ScreenTimeSettings = {
  dailyLimit: 120, // 2 hours
  videoLimit: 60, // 1 hour for videos
  bookLimit: 0, // unlimited for books
  bedtimeStart: "21:00",
  bedtimeEnd: "07:00",
  weekendExtension: 30, // extra 30 minutes on weekends
  enabled: true,
};

export default function ParentalControlsPage() {
  const [user, setUser] = useState<User | null>(null);
  const [role, setRole] = useState<string | null>(null);
  const [children, setChildren] = useState<ChildOrStudent[]>([]);
  const [childSettings, setChildSettings] = useState<ChildSettings[]>([]);
  const [selectedChildId, setSelectedChildId] = useState<string>("");
  const [loading, setLoading] = useState(false);

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
    
    // Load settings for each child
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
      const status = calculateStatus(settings, usage);
      
      return {
        ...settings,
        childId: child.id,
        childName: child.fullName,
        currentUsage: usage,
        status,
      };
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
      };
    } catch (error) {
      console.error("Failed to load usage data:", error);
      return {
        date: today,
        videoMinutes: 0,
        bookMinutes: 0,
        totalMinutes: 0,
        lastActivity: Timestamp.now(),
      };
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
      // Overnight bedtime (e.g., 22:00 to 07:00)
      if (currentTime >= bedtimeStart || currentTime <= bedtimeEnd) {
        return 'bedtime';
      }
    } else {
      // Same day bedtime (e.g., 14:00 to 16:00)
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

  const updateChildSettings = async (childId: string, newSettings: ScreenTimeSettings) => {
    setLoading(true);
    try {
      await setDoc(doc(db, "users", childId, "settings", "screenTime"), newSettings);
      
      // Update local state
      setChildSettings(prev => 
        prev.map(child => 
          child.childId === childId 
            ? { ...child, ...newSettings, status: calculateStatus(newSettings, child.currentUsage!) }
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
      case 'within-limits': return 'text-green-600 bg-green-50';
      case 'approaching-limit': return 'text-yellow-600 bg-yellow-50';
      case 'limit-exceeded': return 'text-red-600 bg-red-50';
      case 'bedtime': return 'text-purple-600 bg-purple-50';
      default: return 'text-gray-600 bg-gray-50';
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

  const selectedChild = childSettings.find(child => child.childId === selectedChildId);

  if (!user) {
    return <p className="p-6">Please log in to access parental controls.</p>;
  }

  if (role !== "parent" && role !== "educator") {
    return <p className="p-6">Access denied. Only parents and educators can access these controls.</p>;
  }

  return (
    <main className="bg-white">
      <div className="max-w-4xl mx-auto p-6 font-sans text-gray-900">
        <div className="flex items-center gap-3 mb-6">
          <Shield className="w-8 h-8 text-blue-600" />
          <h1 className="text-2xl font-bold">
            {role === "parent" ? "Parental Controls" : "Student Screen Time"}
          </h1>
        </div>

        {children.length === 0 ? (
          <div className="text-center py-12">
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
            {/* Overview Cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-8">
              {childSettings.map((child) => (
                <div key={child.childId} className="bg-white border border-gray-200 rounded-xl p-4">
                  <div className="flex items-center justify-between mb-3">
                    <h3 className="font-semibold">{child.childName}</h3>
                    <span className={`px-2 py-1 rounded-full text-xs font-medium flex items-center gap-1 ${getStatusColor(child.status)}`}>
                      {getStatusIcon(child.status)}
                      {getStatusText(child.status)}
                    </span>
                  </div>
                  
                  {child.currentUsage && (
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span>Today's Usage:</span>
                        <span className="font-medium">{formatMinutes(child.currentUsage.totalMinutes)}</span>
                      </div>
                      <div className="flex justify-between text-sm text-gray-600">
                        <span className="flex items-center gap-1">
                          <Play className="w-3 h-3" />
                          Videos:
                        </span>
                        <span>{formatMinutes(child.currentUsage.videoMinutes)}</span>
                      </div>
                      <div className="flex justify-between text-sm text-gray-600">
                        <span className="flex items-center gap-1">
                          <BookOpen className="w-3 h-3" />
                          Books:
                        </span>
                        <span>{formatMinutes(child.currentUsage.bookMinutes)}</span>
                      </div>
                      
                      {child.enabled && (
                        <div className="mt-3">
                          <div className="flex justify-between text-xs text-gray-500 mb-1">
                            <span>Daily Limit</span>
                            <span>{formatMinutes(child.dailyLimit)}</span>
                          </div>
                          <div className="w-full bg-gray-200 rounded-full h-2">
                            <div 
                              className={`h-2 rounded-full transition-all ${
                                child.status === 'limit-exceeded' ? 'bg-red-500' :
                                child.status === 'approaching-limit' ? 'bg-yellow-500' : 'bg-green-500'
                              }`}
                              style={{ 
                                width: `${Math.min(100, (child.currentUsage.totalMinutes / child.dailyLimit) * 100)}%` 
                              }}
                            />
                          </div>
                        </div>
                      )}
                    </div>
                  )}
                  
                  <button
                    onClick={() => setSelectedChildId(child.childId)}
                    className={`mt-3 w-full px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
                      selectedChildId === child.childId
                        ? 'bg-blue-600 text-white'
                        : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                    }`}
                  >
                    {selectedChildId === child.childId ? 'Selected' : 'Manage'}
                  </button>
                </div>
              ))}
            </div>

            {/* Detailed Settings for Selected Child */}
            {selectedChild && (
              <div className="bg-white border border-gray-200 rounded-xl p-6">
                <h2 className="text-xl font-semibold mb-6">
                  Settings for {selectedChild.childName}
                </h2>
                
                <div className="space-y-6">
                  {/* Enable/Disable */}
                  <div className="flex items-center justify-between">
                    <div>
                      <h3 className="font-medium">Screen Time Controls</h3>
                      <p className="text-sm text-gray-600">Enable or disable screen time limits</p>
                    </div>
                    <label className="relative inline-flex items-center cursor-pointer">
                      <input
                        type="checkbox"
                        checked={selectedChild.enabled}
                        onChange={(e) => updateChildSettings(selectedChild.childId, { 
                          ...selectedChild, 
                          enabled: e.target.checked 
                        })}
                        className="sr-only peer"
                        disabled={loading}
                      />
                      <div className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-300 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-blue-600"></div>
                    </label>
                  </div>

                  {selectedChild.enabled && (
                    <>
                      {/* Daily Limits */}
                      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                        <div>
                          <label className="block text-sm font-medium mb-2">
                            Daily Limit (minutes)
                          </label>
                          <input
                            type="number"
                            min="0"
                            max="1440"
                            value={selectedChild.dailyLimit}
                            onChange={(e) => updateChildSettings(selectedChild.childId, { 
                              ...selectedChild, 
                              dailyLimit: parseInt(e.target.value) || 0
                            })}
                            className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                            disabled={loading}
                          />
                        </div>
                        
                        <div>
                          <label className="block text-sm font-medium mb-2">
                            Video Limit (minutes)
                          </label>
                          <input
                            type="number"
                            min="0"
                            max="1440"
                            value={selectedChild.videoLimit}
                            onChange={(e) => updateChildSettings(selectedChild.childId, { 
                              ...selectedChild, 
                              videoLimit: parseInt(e.target.value) || 0
                            })}
                            className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                            disabled={loading}
                          />
                        </div>
                        
                        <div>
                          <label className="block text-sm font-medium mb-2">
                            Weekend Extension (minutes)
                          </label>
                          <input
                            type="number"
                            min="0"
                            max="240"
                            value={selectedChild.weekendExtension}
                            onChange={(e) => updateChildSettings(selectedChild.childId, { 
                              ...selectedChild, 
                              weekendExtension: parseInt(e.target.value) || 0
                            })}
                            className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                            disabled={loading}
                          />
                        </div>
                      </div>

                      {/* Bedtime Settings */}
                      <div>
                        <h3 className="font-medium mb-3">Bedtime Hours</h3>
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                          <div>
                            <label className="block text-sm font-medium mb-2">
                              Bedtime Start
                            </label>
                            <input
                              type="time"
                              value={selectedChild.bedtimeStart}
                              onChange={(e) => updateChildSettings(selectedChild.childId, { 
                                ...selectedChild, 
                                bedtimeStart: e.target.value
                              })}
                              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                              disabled={loading}
                            />
                          </div>
                          
                          <div>
                            <label className="block text-sm font-medium mb-2">
                              Bedtime End
                            </label>
                            <input
                              type="time"
                              value={selectedChild.bedtimeEnd}
                              onChange={(e) => updateChildSettings(selectedChild.childId, { 
                                ...selectedChild, 
                                bedtimeEnd: e.target.value
                              })}
                              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                              disabled={loading}
                            />
                          </div>
                        </div>
                        <p className="text-sm text-gray-600 mt-2">
                          Content will be blocked during these hours. Use 24-hour format.
                        </p>
                      </div>

                      {/* Book Settings Note */}
                      <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
                        <div className="flex items-start gap-3">
                          <BookOpen className="w-5 h-5 text-blue-600 mt-0.5" />
                          <div>
                            <h4 className="font-medium text-blue-900">Reading Encouragement</h4>
                            <p className="text-sm text-blue-800 mt-1">
                              Books have unlimited time by default to encourage reading. Video limits help balance screen time while promoting educational content.
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
    </main>
  );
}