"use client";

import { useState, useEffect, useCallback } from "react";

/* ==================== TYPES ==================== */
type CategoryType = "books" | "videos";

interface FileItem {
  name: string;
  path: string;
  sha: string;
  title?: string;
}

interface ScheduledItem {
  id: string;
  title: string;
  authors: string[];
  categories: string[];
  synopsis: string;
  thumbnail: string;
  link: string;
  category: CategoryType;
  scheduledDate: string;
  status: "pending" | "published" | "failed";
}

/* ==================== CONSTANTS ==================== */
const DISCOVERY_CATEGORIES = [
  "Juvenile Fiction",
  "Early Readers",
  "Educational",
  "Art",
  "Entertainment",
];

/* ==================== MAIN COMPONENT ==================== */
export default function ContentManagerPage() {
  const [activeTab, setActiveTab] = useState<"publish" | "update" | "remove" | "schedule">("publish");

  const tabs = [
    { id: "publish" as const, label: "Publish", icon: "📤" },
    { id: "schedule" as const, label: "Schedule", icon: "📅" },
    { id: "update" as const, label: "Update", icon: "✏️" },
    { id: "remove" as const, label: "Remove", icon: "🗑️" },
  ];

  return (
    <section className="bg-gradient-to-br from-gray-50 to-gray-100 min-h-screen flex">
      {/* Sidebar */}
      <aside className="w-64 bg-white shadow-lg flex flex-col border-r border-gray-200">
        <div className="p-6 border-b border-gray-200">
          <h2 className="text-2xl font-bold text-transparent bg-clip-text bg-gradient-to-r from-pink-600 to-purple-600">
            Content Manager
          </h2>
          <p className="text-sm text-gray-500 mt-1">Manage your library</p>
        </div>
        
        <nav className="flex-1 p-4 space-y-2">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`w-full px-4 py-3 text-left rounded-lg font-semibold transition-all duration-200 flex items-center gap-3 ${
                activeTab === tab.id
                  ? "bg-gradient-to-r from-pink-600 to-purple-600 text-white shadow-md transform scale-105"
                  : "bg-gray-100 text-gray-700 hover:bg-gray-200"
              }`}
            >
              <span className="text-xl">{tab.icon}</span>
              <span>{tab.label}</span>
            </button>
          ))}
        </nav>
      </aside>

      {/* Main Content */}
      <main className="flex-1 p-8 overflow-y-auto">
        <div className="max-w-5xl mx-auto">
          {activeTab === "publish" && <PublishForm />}
          {activeTab === "schedule" && <ScheduleForm />}
          {activeTab === "update" && <UpdateForm />}
          {activeTab === "remove" && <RemoveForm />}
        </div>
      </main>
    </section>
  );
}

/* ==================== PUBLISH FORM ==================== */
function PublishForm() {
  const [category, setCategory] = useState<CategoryType>("books");
  const [title, setTitle] = useState("");
  const [authors, setAuthors] = useState("");
  const [selectedCategories, setSelectedCategories] = useState<string[]>([]);
  const [synopsis, setSynopsis] = useState("");
  const [thumbnail, setThumbnail] = useState("");
  const [link, setLink] = useState("");
  const [message, setMessage] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [showPreview, setShowPreview] = useState(false);

  const generateId = () => title.trim().replace(/\s+/g, "-").toLowerCase();

  const handleCategoryToggle = (cat: string) => {
    setSelectedCategories((prev) =>
      prev.includes(cat) ? prev.filter((c) => c !== cat) : [...prev, cat]
    );
  };

  const resetForm = () => {
    setTitle("");
    setAuthors("");
    setSelectedCategories([]);
    setSynopsis("");
    setThumbnail("");
    setLink("");
  };

  const handlePublish = async () => {
    if (!title.trim() || !authors.trim()) {
      setMessage("❌ Title and Authors are required");
      return;
    }

    setLoading(true);
    setMessage(null);

    const id = generateId();
    const filename = `${id}.json`;
    const newItem = {
      id,
      title: title.trim(),
      authors: authors.split(",").map((a) => a.trim()).filter(Boolean),
      categories: selectedCategories,
      synopsis: synopsis.trim(),
      thumbnail: thumbnail.trim(),
      link: link.trim(),
    };

    try {
      const res = await fetch("/api/github/publish", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ category, filename, content: newItem }),
      });

      const text = await res.text();
      let data: any;
      
      try {
        data = JSON.parse(text);
      } catch {
        data = { error: "Invalid JSON response", details: text };
      }

      if (!res.ok) {
        setMessage(`❌ Publish failed: ${data.error}\n${data.details || ""}`);
      } else {
        setMessage(`✅ Successfully published "${title}" to ${category}`);
        resetForm();
        setTimeout(() => setMessage(null), 5000);
      }
    } catch (err: any) {
      setMessage("❌ Error: " + err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="bg-white rounded-2xl shadow-lg border border-gray-200 overflow-hidden">
      <div className="bg-gradient-to-r from-pink-600 to-purple-600 p-6">
        <h2 className="text-2xl font-bold text-white flex items-center gap-2">
          <span>📤</span> Publish New Content
        </h2>
        <p className="text-pink-100 mt-1">Add new books or videos to your library</p>
      </div>

      <form onSubmit={(e) => { e.preventDefault(); handlePublish(); }} className="p-6 space-y-6">
        <CategoryToggle category={category} setCategory={setCategory} />

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <InputField label="Title *" value={title} onChange={setTitle} placeholder="Enter title" required />
          <InputField label="Authors *" value={authors} onChange={setAuthors} placeholder="John Doe, Jane Smith" required />
        </div>

        <CheckboxGroup selected={selectedCategories} toggle={handleCategoryToggle} />

        <TextAreaField label="Synopsis" value={synopsis} onChange={setSynopsis} placeholder="Brief description..." rows={4} />

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <InputField label="Thumbnail URL" value={thumbnail} onChange={setThumbnail} placeholder="https://example.com/image.jpg" type="url" />
          <InputField label="Content Link" value={link} onChange={setLink} placeholder="https://example.com/content" type="url" />
        </div>

        <div className="flex gap-3 pt-4">
          <button
            type="button"
            onClick={() => {
              if (!title.trim() || !authors.trim()) {
                setMessage("❌ Title and Authors are required");
                return;
              }
              setShowPreview(true);
            }}
            className="flex-1 py-3 bg-gradient-to-r from-blue-600 to-indigo-600 text-white rounded-lg font-semibold hover:shadow-lg hover:transform hover:scale-105 transition-all"
          >
            👁️ Preview
          </button>
          <button
            type="submit"
            disabled={loading}
            className={`flex-1 py-3 rounded-lg font-semibold transition-all duration-200 ${
              loading ? "bg-gray-400 text-white cursor-not-allowed" : "bg-gradient-to-r from-pink-600 to-purple-600 text-white hover:shadow-lg hover:transform hover:scale-105"
            }`}
          >
            {loading ? "Publishing..." : "Publish Now"}
          </button>
          <button type="button" onClick={resetForm} className="px-6 py-3 border-2 border-gray-300 text-gray-700 rounded-lg font-semibold hover:bg-gray-100 transition">
            Clear
          </button>
        </div>

        {message && <ResponseMessage message={message} />}
      </form>

      {showPreview && (
        <PreviewModal
          content={{
            id: generateId(),
            title: title.trim(),
            authors: authors.split(",").map((a) => a.trim()).filter(Boolean),
            categories: selectedCategories,
            synopsis: synopsis.trim(),
            thumbnail: thumbnail.trim(),
            link: link.trim(),
          }}
          category={category}
          onClose={() => setShowPreview(false)}
          onPublish={handlePublish}
        />
      )}
    </div>
  );
}

/* ==================== SCHEDULE FORM ==================== */
function ScheduleForm() {
  const [category, setCategory] = useState<CategoryType>("books");
  const [title, setTitle] = useState("");
  const [authors, setAuthors] = useState("");
  const [selectedCategories, setSelectedCategories] = useState<string[]>([]);
  const [synopsis, setSynopsis] = useState("");
  const [thumbnail, setThumbnail] = useState("");
  const [link, setLink] = useState("");
  const [scheduledDate, setScheduledDate] = useState("");
  const [scheduledTime, setScheduledTime] = useState("");
  const [message, setMessage] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [scheduledItems, setScheduledItems] = useState<ScheduledItem[]>([]);
  const [showScheduled, setShowScheduled] = useState(false);
  const [filter, setFilter] = useState<"all" | "pending" | "published" | "failed">("all");

  const generateId = () => title.trim().replace(/\s+/g, "-").toLowerCase();
  const getCurrentDateTime = () => {
    const now = new Date();
    const date = now.toISOString().split('T')[0];
    const time = now.toTimeString().split(' ')[0].substring(0, 5);
    return { date, time };
  };

  const handleCategoryToggle = (cat: string) => {
    setSelectedCategories((prev) =>
      prev.includes(cat) ? prev.filter((c) => c !== cat) : [...prev, cat]
    );
  };

  const resetForm = () => {
    setTitle("");
    setAuthors("");
    setSelectedCategories([]);
    setSynopsis("");
    setThumbnail("");
    setLink("");
    setScheduledDate("");
    setScheduledTime("");
  };

  const handleSchedule = async () => {
    if (!title.trim() || !authors.trim()) {
      setMessage("❌ Title and Authors are required");
      return;
    }

    if (!scheduledDate || !scheduledTime) {
      setMessage("❌ Please select both date and time");
      return;
    }

    const scheduledDateTime = new Date(`${scheduledDate}T${scheduledTime}`);
    const now = new Date();

    if (scheduledDateTime <= now) {
      setMessage("❌ Scheduled time must be in the future");
      return;
    }

    setLoading(true);
    setMessage(null);

    const scheduledItem: ScheduledItem = {
      id: generateId(),
      title: title.trim(),
      authors: authors.split(",").map((a) => a.trim()).filter(Boolean),
      categories: selectedCategories,
      synopsis: synopsis.trim(),
      thumbnail: thumbnail.trim(),
      link: link.trim(),
      category,
      scheduledDate: scheduledDateTime.toISOString(),
      status: "pending"
    };

    try {
      setScheduledItems(prev => [...prev, scheduledItem]);
      setMessage(`✅ Scheduled "${title}" for ${scheduledDateTime.toLocaleString()}`);
      resetForm();
      setTimeout(() => setMessage(null), 5000);
    } catch (err: any) {
      setMessage("❌ Error: " + err.message);
    } finally {
      setLoading(false);
    }
  };

  const handlePublishNow = async (item: ScheduledItem) => {
    setMessage(null);
    
    const filename = `${item.id}.json`;
    const content = {
      id: item.id,
      title: item.title,
      authors: item.authors,
      categories: item.categories,
      synopsis: item.synopsis,
      thumbnail: item.thumbnail,
      link: item.link,
    };

    try {
      const res = await fetch("/api/github/publish", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ category: item.category, filename, content }),
      });

      if (res.ok) {
        setScheduledItems(prev => prev.map(si => si.id === item.id ? { ...si, status: "published" as const } : si));
        setMessage(`✅ Published "${item.title}" successfully`);
      } else {
        const data = await res.json();
        setScheduledItems(prev => prev.map(si => si.id === item.id ? { ...si, status: "failed" as const } : si));
        setMessage(`❌ Publish failed: ${data.error}`);
      }
    } catch (err: any) {
      setMessage("❌ Error: " + err.message);
    }
  };

  const handleDeleteScheduled = (itemId: string) => {
    setScheduledItems(prev => prev.filter(item => item.id !== itemId));
    setMessage(`✅ Deleted scheduled item`);
    setTimeout(() => setMessage(null), 3000);
  };

  const checkAndPublishScheduled = useCallback(() => {
    const now = new Date();
    scheduledItems.forEach(item => {
      if (item.status === "pending" && new Date(item.scheduledDate) <= now) {
        handlePublishNow(item);
      }
    });
  }, [scheduledItems]);

  useEffect(() => {
    const interval = setInterval(checkAndPublishScheduled, 60000);
    return () => clearInterval(interval);
  }, [checkAndPublishScheduled]);

  const filteredItems = scheduledItems.filter(item => filter === "all" || item.status === filter);
  const counts = {
    pending: scheduledItems.filter(i => i.status === "pending").length,
    published: scheduledItems.filter(i => i.status === "published").length,
    failed: scheduledItems.filter(i => i.status === "failed").length,
  };

  return (
    <div className="bg-white rounded-2xl shadow-lg border border-gray-200 overflow-hidden">
      <div className="bg-gradient-to-r from-pink-600 to-purple-600 p-6 flex justify-between items-center">
        <div>
          <h2 className="text-2xl font-bold text-white flex items-center gap-2">
            <span>📅</span> Schedule Content
          </h2>
          <p className="text-pink-100 mt-1">Plan content for automatic publishing</p>
        </div>
        <button
          onClick={() => setShowScheduled(!showScheduled)}
          className="px-4 py-2 bg-white text-pink-600 rounded-lg font-semibold hover:bg-pink-50 transition flex items-center gap-2"
        >
          {showScheduled ? "📝 New Schedule" : `📋 View Scheduled (${scheduledItems.length})`}
        </button>
      </div>

      <div className="p-6">
        {!showScheduled ? (
          <form onSubmit={(e) => { e.preventDefault(); handleSchedule(); }} className="space-y-6">
            <CategoryToggle category={category} setCategory={setCategory} />

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <InputField label="Title *" value={title} onChange={setTitle} placeholder="Enter title" required />
              <InputField label="Authors *" value={authors} onChange={setAuthors} placeholder="John Doe, Jane Smith" required />
            </div>

            <CheckboxGroup selected={selectedCategories} toggle={handleCategoryToggle} />

            <TextAreaField label="Synopsis" value={synopsis} onChange={setSynopsis} placeholder="Brief description..." rows={3} />

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <InputField label="Thumbnail URL" value={thumbnail} onChange={setThumbnail} placeholder="https://example.com/image.jpg" type="url" />
              <InputField label="Content Link" value={link} onChange={setLink} placeholder="https://example.com/content" type="url" />
            </div>

            <div className="bg-purple-50 p-4 rounded-lg border border-purple-200">
              <p className="text-sm font-semibold text-purple-900 mb-3">Schedule Settings</p>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <InputField label="📅 Date *" value={scheduledDate} onChange={setScheduledDate} type="date" min={getCurrentDateTime().date} required />
                <InputField label="⏰ Time *" value={scheduledTime} onChange={setScheduledTime} type="time" required />
              </div>
            </div>

            <div className="flex gap-3 pt-4">
              <button
                type="submit"
                disabled={loading}
                className={`flex-1 py-3 rounded-lg font-semibold transition-all duration-200 ${
                  loading ? "bg-gray-400 text-white cursor-not-allowed" : "bg-gradient-to-r from-pink-600 to-purple-600 text-white hover:shadow-lg hover:transform hover:scale-105"
                }`}
              >
                {loading ? "Scheduling..." : "Schedule for Later"}
              </button>
              <button type="button" onClick={resetForm} className="px-6 py-3 border-2 border-gray-300 text-gray-700 rounded-lg font-semibold hover:bg-gray-100 transition">
                Clear
              </button>
            </div>

            {message && <ResponseMessage message={message} />}
          </form>
        ) : (
          <div className="space-y-4">
            <div className="flex gap-2 border-b border-gray-200 pb-3">
              {(["all", "pending", "published", "failed"] as const).map((status) => (
                <button
                  key={status}
                  onClick={() => setFilter(status)}
                  className={`px-4 py-2 rounded-lg font-medium transition ${
                    filter === status ? "bg-pink-600 text-white" : "bg-gray-100 text-gray-700 hover:bg-gray-200"
                  }`}
                >
                  {status.charAt(0).toUpperCase() + status.slice(1)}
                  {status !== "all" && ` (${counts[status]})`}
                </button>
              ))}
            </div>

            <div className="space-y-3 max-h-96 overflow-y-auto">
              {filteredItems.map((item) => (
                <ScheduledItemCard key={item.id} item={item} onPublish={handlePublishNow} onDelete={handleDeleteScheduled} />
              ))}
              {filteredItems.length === 0 && (
                <div className="text-center py-12">
                  <p className="text-gray-500 text-lg">No {filter} items</p>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function ScheduledItemCard({ item, onPublish, onDelete }: { item: ScheduledItem; onPublish: (item: ScheduledItem) => void; onDelete: (id: string) => void; }) {
  const statusConfig = {
    pending: { bg: "bg-yellow-50", border: "border-yellow-200", badge: "bg-yellow-100 text-yellow-800" },
    published: { bg: "bg-green-50", border: "border-green-200", badge: "bg-green-100 text-green-800" },
    failed: { bg: "bg-red-50", border: "border-red-200", badge: "bg-red-100 text-red-800" },
  };

  const config = statusConfig[item.status];
  const scheduledDate = new Date(item.scheduledDate);

  return (
    <div className={`${config.bg} border ${config.border} rounded-lg p-4`}>
      <div className="flex justify-between items-start gap-4">
        <div className="flex-1">
          <h4 className="font-semibold text-gray-900 text-lg">{item.title}</h4>
          <p className="text-sm text-gray-600 mt-1">by {item.authors.join(", ")} • {item.category}</p>
          <div className="flex items-center gap-2 mt-2">
            <span className={`text-xs px-2 py-1 rounded-full font-medium ${config.badge}`}>{item.status.toUpperCase()}</span>
            <span className="text-xs text-gray-500">{scheduledDate.toLocaleString()}</span>
          </div>
        </div>
        <div className="flex gap-2">
          {item.status === "pending" && (
            <button onClick={() => onPublish(item)} className="px-3 py-2 bg-green-600 text-white text-sm rounded-lg hover:bg-green-700 transition font-medium">
              Publish Now
            </button>
          )}
          {item.status === "failed" && (
            <button onClick={() => onPublish(item)} className="px-3 py-2 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700 transition font-medium">
              Retry
            </button>
          )}
          <button onClick={() => onDelete(item.id)} className="px-3 py-2 bg-red-600 text-white text-sm rounded-lg hover:bg-red-700 transition font-medium">
            Delete
          </button>
        </div>
      </div>
    </div>
  );
}

/* ==================== UPDATE FORM ==================== */
function UpdateForm() {
  const [category, setCategory] = useState<CategoryType>("books");
  const [files, setFiles] = useState<FileItem[]>([]);
  const [selectedFile, setSelectedFile] = useState<FileItem | null>(null);
  const [formData, setFormData] = useState<any>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [showConfirm, setShowConfirm] = useState(false);
  const [loading, setLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");

  useEffect(() => {
    fetchFiles();
  }, [category]);

  const fetchFiles = async () => {
    setLoading(true);
    try {
      const res = await fetch(`/api/github/list-files?category=${category}`);
      const filesData: FileItem[] = await res.json();

      const filesWithTitles = await Promise.all(
        filesData.map(async (file) => {
          try {
            const fileRes = await fetch(`/api/github/get-file?path=${encodeURIComponent(file.path)}`);
            const data = await fileRes.json();
            return { ...file, title: data.title || file.name };
          } catch {
            return { ...file, title: file.name };
          }
        })
      );

      setFiles(filesWithTitles);
      setSelectedFile(null);
      setFormData(null);
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (!selectedFile) return;
    fetch(`/api/github/get-file?path=${encodeURIComponent(selectedFile.path)}`)
      .then((res) => res.json())
      .then((data) => setFormData({ ...data, categories: data.categories || [] }))
      .catch(console.error);
  }, [selectedFile]);

  const handleFieldChange = (field: string, value: string | string[]) => {
    setFormData((prev: any) => ({ ...prev, [field]: value }));
  };

  const toggleCategory = (cat: string) => {
    if (!formData) return;
    const current = formData.categories || [];
    handleFieldChange("categories", current.includes(cat) ? current.filter((c: string) => c !== cat) : [...current, cat]);
  };

  const handleUpdate = async () => {
    if (!selectedFile || !formData) return;

    setShowConfirm(false);
    setMessage(null);

    try {
      const res = await fetch("/api/github/update", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ category, filename: selectedFile.name, content: formData, sha: selectedFile.sha }),
      });

      const data = await res.json();
      if (res.ok) {
        setMessage("✅ Update successful!");
        setTimeout(() => setMessage(null), 5000);
      } else {
        setMessage("❌ Update failed: " + data.error);
      }
    } catch (err: any) {
      setMessage("❌ Error: " + err.message);
    }
  };

  const filteredFiles = files.filter(file => 
    file.title?.toLowerCase().includes(searchQuery.toLowerCase()) ||
    file.name.toLowerCase().includes(searchQuery.toLowerCase())
  );

  return (
    <div className="bg-white rounded-2xl shadow-lg border border-gray-200 overflow-hidden">
      <div className="bg-gradient-to-r from-pink-600 to-purple-600 p-6">
        <h2 className="text-2xl font-bold text-white flex items-center gap-2">
          <span>✏️</span> Update Content
        </h2>
        <p className="text-pink-100 mt-1">Edit existing books or videos</p>
      </div>

      <div className="p-6 space-y-6">
        <CategoryToggle category={category} setCategory={setCategory} />

        <InputField label="🔍 Search Content" value={searchQuery} onChange={setSearchQuery} placeholder="Search by title or filename..." />

        <div>
          <p className="text-sm font-semibold text-gray-700 mb-3">Select content to edit ({filteredFiles.length} items)</p>
          {loading ? (
            <div className="text-center py-8"><p className="text-gray-500">Loading {category}...</p></div>
          ) : (
            <div className="space-y-2 max-h-64 overflow-y-auto">
              {filteredFiles.map((file) => (
                <button
                  key={file.sha}
                  onClick={() => setSelectedFile(selectedFile?.name === file.name ? null : file)}
                  className={`w-full text-left p-4 rounded-lg border-2 transition-all ${
                    selectedFile?.name === file.name
                      ? "bg-pink-50 border-pink-400 shadow-md transform scale-102"
                      : "bg-white border-gray-200 hover:border-pink-300 hover:bg-gray-50"
                  }`}
                >
                  <p className="font-medium text-gray-900">{file.title}</p>
                  <p className="text-xs text-gray-500 mt-1">{file.name}</p>
                </button>
              ))}
              {filteredFiles.length === 0 && (
                <p className="text-center py-8 text-gray-500">{searchQuery ? "No matching content found" : `No ${category} found`}</p>
              )}
            </div>
          )}
        </div>

        {formData && selectedFile && (
          <div className="border-t pt-6 space-y-4">
            <p className="text-sm font-semibold text-gray-700 mb-3">📝 Edit Details</p>
            
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <InputField label="Title" value={formData.title || ""} onChange={(v) => handleFieldChange("title", v)} />
              <InputField 
                label="Authors" 
                value={formData.authors?.join(", ") || ""} 
                onChange={(v) => handleFieldChange("authors", v.split(",").map((a) => a.trim()))} 
              />
            </div>

            <div>
              <p className="text-sm font-medium text-gray-700 mb-2">Categories</p>
              <div className="flex flex-wrap gap-2">
                {DISCOVERY_CATEGORIES.map((cat) => (
                  <button
                    key={cat}
                    type="button"
                    onClick={() => toggleCategory(cat)}
                    className={`px-4 py-2 border-2 rounded-lg font-medium transition-all ${
                      formData.categories?.includes(cat)
                        ? "bg-pink-600 text-white border-pink-600"
                        : "bg-white text-gray-700 border-gray-300 hover:border-pink-400"
                    }`}
                  >
                    {cat}
                  </button>
                ))}
              </div>
            </div>

            <TextAreaField label="Synopsis" value={formData.synopsis || ""} onChange={(v) => handleFieldChange("synopsis", v)} rows={4} />

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <InputField label="Thumbnail URL" value={formData.thumbnail || ""} onChange={(v) => handleFieldChange("thumbnail", v)} />
              <InputField label="Content Link" value={formData.link || ""} onChange={(v) => handleFieldChange("link", v)} />
            </div>

            <button
              type="button"
              onClick={() => setShowConfirm(true)}
              className="w-full py-3 bg-gradient-to-r from-pink-600 to-purple-600 text-white rounded-lg font-semibold hover:shadow-lg hover:transform hover:scale-105 transition-all"
            >
              Save Changes
            </button>
          </div>
        )}

        {message && <ResponseMessage message={message} />}
      </div>

      {showConfirm && (
        <ConfirmModal
          title="Confirm Update"
          message={`Save changes to "${selectedFile?.title || selectedFile?.name}"?`}
          onConfirm={handleUpdate}
          onCancel={() => setShowConfirm(false)}
        />
      )}
    </div>
  );
}

/* ==================== REMOVE FORM ==================== */
function RemoveForm() {
  const [category, setCategory] = useState<CategoryType>("books");
  const [files, setFiles] = useState<FileItem[]>([]);
  const [message, setMessage] = useState<string | null>(null);
  const [confirmFile, setConfirmFile] = useState<FileItem | null>(null);
  const [loading, setLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");

  useEffect(() => {
    fetchFiles();
  }, [category]);

  const fetchFiles = async () => {
    setLoading(true);
    try {
      const res = await fetch(`/api/github/list-files?category=${category}`);
      const filesData: FileItem[] = await res.json();

      const filesWithTitles = await Promise.all(
        filesData.map(async (file) => {
          try {
            const fileRes = await fetch(`/api/github/get-file?path=${encodeURIComponent(file.path)}`);
            const data = await fileRes.json();
            return { ...file, title: data.title || file.name };
          } catch {
            return { ...file, title: file.name };
          }
        })
      );
      setFiles(filesWithTitles);
    } catch (err) {
      setMessage("❌ Failed to load files");
    } finally {
      setLoading(false);
    }
  };

  const handleRemove = async (file: FileItem) => {
    setMessage(null);
    try {
      const res = await fetch("/api/github/remove", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ category, filename: file.name, sha: file.sha }),
      });
      const data = await res.json();
      if (res.ok) {
        setMessage(`✅ Removed "${file.title || file.name}"`);
        setFiles(files.filter((f) => f.name !== file.name));
        setTimeout(() => setMessage(null), 5000);
      } else {
        setMessage("❌ Remove failed: " + data.error);
      }
    } catch (err: any) {
      setMessage("❌ Error: " + err.message);
    } finally {
      setConfirmFile(null);
    }
  };

  const filteredFiles = files.filter(file => 
    file.title?.toLowerCase().includes(searchQuery.toLowerCase()) ||
    file.name.toLowerCase().includes(searchQuery.toLowerCase())
  );

  return (
    <div className="bg-white rounded-2xl shadow-lg border border-gray-200 overflow-hidden">
      <div className="bg-gradient-to-r from-red-600 to-pink-600 p-6">
        <h2 className="text-2xl font-bold text-white flex items-center gap-2">
          <span>🗑️</span> Remove Content
        </h2>
        <p className="text-red-100 mt-1">Delete books or videos from your library</p>
      </div>

      <div className="p-6 space-y-6">
        <CategoryToggle category={category} setCategory={setCategory} />

        <InputField label="🔍 Search Content" value={searchQuery} onChange={setSearchQuery} placeholder="Search by title or filename..." />

        {loading ? (
          <div className="text-center py-8"><p className="text-gray-500">Loading {category}...</p></div>
        ) : (
          <div className="space-y-3 max-h-96 overflow-y-auto">
            {filteredFiles.map((file) => (
              <div key={file.sha} className="flex justify-between items-center border-2 border-gray-200 p-4 rounded-lg hover:border-red-300 hover:bg-red-50 transition-all">
                <div>
                  <p className="font-semibold text-gray-900">{file.title || file.name}</p>
                  <p className="text-xs text-gray-500 mt-1">{file.name}</p>
                </div>
                <button onClick={() => setConfirmFile(file)} className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition font-medium">
                  Remove
                </button>
              </div>
            ))}
            {filteredFiles.length === 0 && (
              <div className="text-center py-12">
                <p className="text-gray-500 text-lg">{searchQuery ? "No matching content found" : `No ${category} found`}</p>
              </div>
            )}
          </div>
        )}

        {message && <ResponseMessage message={message} />}
      </div>

      {confirmFile && (
        <ConfirmModal
          title="Confirm Removal"
          message={`Are you sure you want to remove "${confirmFile.title || confirmFile.name}"? This action cannot be undone.`}
          onConfirm={() => handleRemove(confirmFile)}
          onCancel={() => setConfirmFile(null)}
          confirmText="Yes, Remove"
          isDangerous
        />
      )}
    </div>
  );
}

/* ==================== REUSABLE COMPONENTS ==================== */
function CategoryToggle({ category, setCategory }: { category: CategoryType; setCategory: (cat: CategoryType) => void; }) {
  return (
    <div className="flex justify-center gap-3">
      {(["books", "videos"] as CategoryType[]).map((type) => (
        <button
          key={type}
          type="button"
          onClick={() => setCategory(type)}
          className={`px-6 py-3 rounded-lg font-semibold transition-all duration-200 ${
            category === type
              ? "bg-gradient-to-r from-pink-600 to-purple-600 text-white shadow-md transform scale-105"
              : "bg-gray-100 text-gray-700 hover:bg-gray-200"
          }`}
        >
          {type === "books" ? "📚" : "🎥"} {type.charAt(0).toUpperCase() + type.slice(1)}
        </button>
      ))}
    </div>
  );
}

function CheckboxGroup({ selected, toggle }: { selected: string[]; toggle: (cat: string) => void; }) {
  return (
    <div>
      <p className="text-sm font-semibold text-gray-700 mb-3">Categories</p>
      <div className="flex flex-wrap gap-2">
        {DISCOVERY_CATEGORIES.map((cat) => (
          <button
            key={cat}
            type="button"
            onClick={() => toggle(cat)}
            className={`px-4 py-2 border-2 rounded-lg font-medium transition-all ${
              selected.includes(cat)
                ? "bg-pink-600 text-white border-pink-600 shadow-sm"
                : "bg-white text-gray-700 border-gray-300 hover:border-pink-400"
            }`}
          >
            {cat}
          </button>
        ))}
      </div>
    </div>
  );
}

function InputField({ label, value, onChange, placeholder = "", type = "text", required = false, min = "" }: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  type?: string;
  required?: boolean;
  min?: string;
}) {
  return (
    <div>
      <label className="block text-sm font-semibold text-gray-700 mb-2">{label}</label>
      <input
        type={type}
        placeholder={placeholder}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="w-full px-4 py-3 border border-gray-300 rounded-lg text-gray-900 focus:ring-2 focus:ring-pink-500 focus:border-transparent transition"
        required={required}
        min={min}
      />
    </div>
  );
}

function TextAreaField({ label, value, onChange, placeholder = "", rows = 4 }: {
  label: string;
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  rows?: number;
}) {
  return (
    <div>
      <label className="block text-sm font-semibold text-gray-700 mb-2">{label}</label>
      <textarea
        placeholder={placeholder}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        rows={rows}
        className="w-full px-4 py-3 text-gray-900 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent transition resize-none"
      />
    </div>
  );
}

function ResponseMessage({ message }: { message: string }) {
  const isSuccess = message.startsWith("✅");
  return (
    <div className={`p-4 rounded-lg border-2 ${isSuccess ? "bg-green-50 border-green-300 text-green-800" : "bg-red-50 border-red-300 text-red-800"}`}>
      <p className="font-medium">{message}</p>
    </div>
  );
}

function ConfirmModal({ title, message, onConfirm, onCancel, confirmText = "Yes, Confirm", isDangerous = false }: {
  title: string;
  message: string;
  onConfirm: () => void;
  onCancel: () => void;
  confirmText?: string;
  isDangerous?: boolean;
}) {
  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
      <div className="bg-white rounded-2xl max-w-md w-full p-6 shadow-2xl transform transition-all">
        <h3 className={`text-2xl font-bold mb-4 ${isDangerous ? "text-red-600" : "text-pink-600"}`}>{title}</h3>
        <p className="text-gray-700 mb-6 leading-relaxed">{message}</p>
        <div className="flex gap-3">
          <button
            onClick={onConfirm}
            className={`flex-1 py-3 rounded-lg font-semibold transition-all ${
              isDangerous ? "bg-red-600 hover:bg-red-700 text-white" : "bg-gradient-to-r from-pink-600 to-purple-600 hover:shadow-lg text-white"
            }`}
          >
            {confirmText}
          </button>
          <button onClick={onCancel} className="flex-1 py-3 bg-gray-200 text-gray-800 rounded-lg font-semibold hover:bg-gray-300 transition">
            Cancel
          </button>
        </div>
      </div>
    </div>
  );
}

function PreviewModal({ content, category, onClose, onPublish }: {
  content: { id: string; title: string; authors: string[]; categories: string[]; synopsis: string; thumbnail: string; link: string; };
  category: CategoryType;
  onClose: () => void;
  onPublish: () => void;
}) {
  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4 overflow-y-auto">
      <div className="bg-white rounded-2xl max-w-3xl w-full my-8 shadow-2xl transform transition-all">
        <div className="bg-gradient-to-r from-blue-600 to-indigo-600 p-6 rounded-t-2xl">
          <h3 className="text-2xl font-bold text-white flex items-center gap-2">👁️ Content Preview</h3>
          <p className="text-blue-100 mt-1">Review before publishing</p>
        </div>

        <div className="p-6 space-y-6 max-h-[70vh] overflow-y-auto">
          <div className="flex items-center gap-2">
            <span className="px-3 py-1 bg-purple-100 text-purple-700 rounded-full text-sm font-semibold">
              {category === "books" ? "📚" : "🎥"} {category.charAt(0).toUpperCase() + category.slice(1)}
            </span>
            <span className="text-xs text-gray-500">ID: {content.id}</span>
          </div>

          <div className="flex gap-6">
            {content.thumbnail ? (
              <div className="flex-shrink-0">
                <img
                  src={content.thumbnail}
                  alt={content.title}
                  className="w-32 h-48 object-cover rounded-lg shadow-md border-2 border-gray-200"
                  onError={(e) => {
                    e.currentTarget.src = "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='128' height='192'%3E%3Crect fill='%23e5e7eb' width='128' height='192'/%3E%3Ctext x='50%25' y='50%25' text-anchor='middle' dy='.3em' fill='%239ca3af' font-family='sans-serif' font-size='14'%3ENo Image%3C/text%3E%3C/svg%3E";
                  }}
                />
              </div>
            ) : (
              <div className="flex-shrink-0 w-32 h-48 bg-gray-200 rounded-lg flex items-center justify-center border-2 border-gray-300">
                <span className="text-gray-400 text-sm">No Image</span>
              </div>
            )}

            <div className="flex-1 space-y-4">
              <div>
                <h4 className="text-2xl font-bold text-gray-900">{content.title}</h4>
                <p className="text-gray-600 mt-1">by {content.authors.length > 0 ? content.authors.join(", ") : "Unknown Author"}</p>
              </div>

              {content.categories.length > 0 && (
                <div>
                  <p className="text-sm font-semibold text-gray-700 mb-2">Categories:</p>
                  <div className="flex flex-wrap gap-2">
                    {content.categories.map((cat) => (
                      <span key={cat} className="px-3 py-1 bg-pink-100 text-pink-700 rounded-full text-sm font-medium">{cat}</span>
                    ))}
                  </div>
                </div>
              )}

              {content.synopsis && (
                <div>
                  <p className="text-sm font-semibold text-gray-700 mb-2">Synopsis:</p>
                  <p className="text-gray-700 leading-relaxed">{content.synopsis}</p>
                </div>
              )}

              {content.link && (
                <div>
                  <p className="text-sm font-semibold text-gray-700 mb-1">Content Link:</p>
                  <a href={content.link} target="_blank" rel="noopener noreferrer" className="text-blue-600 hover:text-blue-800 underline break-all text-sm">
                    {content.link}
                  </a>
                </div>
              )}
            </div>
          </div>

          <div>
            <p className="text-sm font-semibold text-gray-700 mb-2">JSON Data:</p>
            <pre className="bg-gray-100 p-4 rounded-lg text-xs overflow-x-auto border border-gray-300">
              {JSON.stringify(content, null, 2)}
            </pre>
          </div>
        </div>

        <div className="p-6 border-t border-gray-200 flex gap-3">
          <button
            onClick={() => { onPublish(); onClose(); }}
            className="flex-1 py-3 bg-gradient-to-r from-pink-600 to-purple-600 text-white rounded-lg font-semibold hover:shadow-lg hover:transform hover:scale-105 transition-all"
          >
            ✅ Looks Good, Publish
          </button>
          <button onClick={onClose} className="flex-1 py-3 bg-gray-200 text-gray-800 rounded-lg font-semibold hover:bg-gray-300 transition">
            ← Back to Edit
          </button>
        </div>
      </div>
    </div>
  );
}