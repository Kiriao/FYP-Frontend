"use client";

import { useState, useEffect } from "react";

/* ------------------ TYPES ------------------ */
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

/* ------------------ DISCOVERY CATEGORIES ------------------ */
const DISCOVERY_CATEGORIES = [
  "Juvenile Fiction",
  "Early Readers",
  "Educational",
  "Art",
  "Entertainment",
];

/* ------------------ MAIN PAGE ------------------ */
export default function ContentManagerPage() {
  const [activeTab, setActiveTab] = useState<"publish" | "update" | "remove" | "schedule">(
    "publish"
  );

  return (
    <section className="bg-gray-100 min-h-screen flex">
      {/* Sidebar */}
      <aside className="w-64 bg-white shadow-md p-6 flex flex-col">
        <h2 className="text-xl font-bold text-pink-600 mb-6">
          Content Manager
        </h2>
        <nav className="flex flex-col gap-2">
          {(["publish", "update", "remove", "schedule"] as const).map((tab) => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`px-4 py-2 text-left rounded-md font-semibold transition ${
                activeTab === tab
                  ? "bg-pink-600 text-white"
                  : "bg-gray-200 text-gray-800 hover:bg-gray-300"
              }`}
            >
              {tab.charAt(0).toUpperCase() + tab.slice(1)}
            </button>
          ))}
        </nav>
      </aside>

      {/* Main Content */}
      <main className="flex-1 p-8">
        {activeTab === "publish" && <PublishForm />}
        {activeTab === "update" && <UpdateForm />}
        {activeTab === "remove" && <RemoveForm />}
        {activeTab === "schedule" && <ScheduleForm />}
      </main>
    </section>
  );
}

/* ------------------ PUBLISH FORM ------------------ */
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

  const generateId = () => title.trim().replace(/\s+/g, "-");

  const handleCategoryToggle = (cat: string) => {
    setSelectedCategories((prev) =>
      prev.includes(cat) ? prev.filter((c) => c !== cat) : [...prev, cat]
    );
  };

  const handlePublish = async () => {
    setLoading(true);
    setMessage(null);

    const id = generateId();
    const filename = `${id}.json`;

    const newItem = {
      id,
      title,
      authors: authors.split(",").map((a) => a.trim()),
      categories: selectedCategories,
      synopsis,
      thumbnail,
      link,
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
        setMessage(`✅ Published ${filename} in ${category}`);
        setTitle("");
        setAuthors("");
        setSelectedCategories([]);
        setSynopsis("");
        setThumbnail("");
        setLink("");
      }
    } catch (err: any) {
      setMessage("❌ Error: " + err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <section className="bg-white py-16 px-6 max-w-3xl mx-auto rounded-xl shadow-md border space-y-6">
      <h2 className="text-2xl font-bold text-pink-600 mb-4 text-center">
        Publish Content
      </h2>
      <form
        onSubmit={(e) => {
          e.preventDefault();
          handlePublish();
        }}
        className="bg-white p-6 rounded-xl shadow-md border border-gray-200 space-y-4 max-w-3xl mx-auto text-gray-800"
      >
        <CategoryToggle category={category} setCategory={setCategory} />

        <input
          type="text"
          placeholder="Title"
          value={title}
          onChange={(e) => setTitle(e.target.value)}
          className="w-full px-4 py-2 border rounded-md text-gray-800"
          required
        />
        <input
          type="text"
          placeholder="Authors (comma separated)"
          value={authors}
          onChange={(e) => setAuthors(e.target.value)}
          className="w-full px-4 py-2 border rounded-md text-gray-800"
          required
        />

        <CheckboxGroup selected={selectedCategories} toggle={handleCategoryToggle} />

        <textarea
          placeholder="Synopsis"
          value={synopsis}
          onChange={(e) => setSynopsis(e.target.value)}
          className="w-full px-4 py-2 border rounded-md text-gray-800"
        />
        <input
          type="text"
          placeholder="Thumbnail URL"
          value={thumbnail}
          onChange={(e) => setThumbnail(e.target.value)}
          className="w-full px-4 py-2 border rounded-md"
        />
        <input
          type="url"
          placeholder="Book / Video Link"
          value={link}
          onChange={(e) => setLink(e.target.value)}
          className="w-full px-4 py-2 border rounded-md"
        />

        <button
          type="submit"
          disabled={loading}
          className={`w-full py-2 rounded-md font-semibold transition ${
            loading
              ? "bg-gray-400 text-white cursor-not-allowed"
              : "bg-pink-600 text-white hover:bg-pink-700"
          }`}
        >
          {loading ? "Publishing..." : "Publish"}
        </button>

        {message && <ResponseMessage message={message} />}
      </form>
    </section>
  );
}

/* ------------------ SCHEDULE FORM ------------------ */
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

  // Load scheduled items on component mount
  useEffect(() => {
    loadScheduledItems();
  }, []);

  const loadScheduledItems = () => {
    // Using in-memory storage instead of localStorage
    setScheduledItems([]);
  };

  const saveScheduledItems = (items: ScheduledItem[]) => {
    setScheduledItems(items);
  };

  const generateId = () => title.trim().replace(/\s+/g, "-");

  const handleCategoryToggle = (cat: string) => {
    setSelectedCategories((prev) =>
      prev.includes(cat) ? prev.filter((c) => c !== cat) : [...prev, cat]
    );
  };

  const getCurrentDateTime = () => {
    const now = new Date();
    const date = now.toISOString().split('T')[0];
    const time = now.toTimeString().split(' ')[0].substring(0, 5);
    return { date, time };
  };

  const handleSchedule = async () => {
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

    const id = generateId();
    const scheduledItem: ScheduledItem = {
      id,
      title,
      authors: authors.split(",").map((a) => a.trim()),
      categories: selectedCategories,
      synopsis,
      thumbnail,
      link,
      category,
      scheduledDate: scheduledDateTime.toISOString(),
      status: "pending"
    };

    try {
      // Save to scheduled items
      const newScheduledItems = [...scheduledItems, scheduledItem];
      saveScheduledItems(newScheduledItems);

      setMessage(`✅ Scheduled "${title}" for ${scheduledDate} at ${scheduledTime}`);
      
      // Clear form
      setTitle("");
      setAuthors("");
      setSelectedCategories([]);
      setSynopsis("");
      setThumbnail("");
      setLink("");
      setScheduledDate("");
      setScheduledTime("");

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
        // Update item status
        const updatedItems = scheduledItems.map(si => 
          si.id === item.id ? { ...si, status: "published" as const } : si
        );
        saveScheduledItems(updatedItems);
        setMessage(`✅ Published "${item.title}" successfully`);
      } else {
        const data = await res.json();
        const updatedItems = scheduledItems.map(si => 
          si.id === item.id ? { ...si, status: "failed" as const } : si
        );
        saveScheduledItems(updatedItems);
        setMessage(`❌ Publish failed: ${data.error}`);
      }
    } catch (err: any) {
      setMessage("❌ Error: " + err.message);
    }
  };

  const handleDeleteScheduled = (itemId: string) => {
    const updatedItems = scheduledItems.filter(item => item.id !== itemId);
    saveScheduledItems(updatedItems);
  };

  const checkAndPublishScheduled = () => {
    const now = new Date();
    scheduledItems.forEach(item => {
      if (item.status === "pending" && new Date(item.scheduledDate) <= now) {
        handlePublishNow(item);
      }
    });
  };

  // Check for items to publish every minute
  useEffect(() => {
    const interval = setInterval(checkAndPublishScheduled, 60000);
    return () => clearInterval(interval);
  }, [scheduledItems]);

  const pendingItems = scheduledItems.filter(item => item.status === "pending");
  const publishedItems = scheduledItems.filter(item => item.status === "published");
  const failedItems = scheduledItems.filter(item => item.status === "failed");

  return (
    <section className="bg-white py-16 px-6 max-w-4xl mx-auto rounded-xl shadow-md border space-y-6">
      <div className="flex justify-between items-center">
        <h2 className="text-2xl font-bold text-pink-600">
          Schedule Content
        </h2>
        <button
          onClick={() => setShowScheduled(!showScheduled)}
          className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition"
        >
          {showScheduled ? "Hide" : "Show"} Scheduled ({scheduledItems.length})
        </button>
      </div>

      {!showScheduled ? (
        <form
          onSubmit={(e) => {
            e.preventDefault();
            handleSchedule();
          }}
          className="space-y-4"
        >
          <CategoryToggle category={category} setCategory={setCategory} />

          <input
            type="text"
            placeholder="Title"
            value={title}
            onChange={(e) => setTitle(e.target.value)}
            className="w-full px-4 py-2 border rounded-md text-gray-800"
            required
          />
          <input
            type="text"
            placeholder="Authors (comma separated)"
            value={authors}
            onChange={(e) => setAuthors(e.target.value)}
            className="w-full px-4 py-2 border rounded-md text-gray-800"
            required
          />

          <CheckboxGroup selected={selectedCategories} toggle={handleCategoryToggle} />

          <textarea
            placeholder="Synopsis"
            value={synopsis}
            onChange={(e) => setSynopsis(e.target.value)}
            className="w-full px-4 py-2 border rounded-md text-gray-800"
            rows={3}
          />
          <input
            type="text"
            placeholder="Thumbnail URL"
            value={thumbnail}
            onChange={(e) => setThumbnail(e.target.value)}
            className="w-full px-4 py-2 border rounded-md text-gray-800"
          />
          <input
            type="url"
            placeholder="Book / Video Link"
            value={link}
            onChange={(e) => setLink(e.target.value)}
            className="w-full px-4 py-2 border rounded-md text-gray-800"
          />

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Schedule Date
              </label>
              <input
                type="date"
                value={scheduledDate}
                onChange={(e) => setScheduledDate(e.target.value)}
                min={getCurrentDateTime().date}
                className="w-full px-4 py-2 border rounded-md text-gray-800"
                required
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Schedule Time
              </label>
              <input
                type="time"
                value={scheduledTime}
                onChange={(e) => setScheduledTime(e.target.value)}
                className="w-full px-4 py-2 border rounded-md text-gray-800"
                required
              />
            </div>
          </div>

          <button
            type="submit"
            disabled={loading}
            className={`w-full py-2 rounded-md font-semibold transition ${
              loading
                ? "bg-gray-400 text-white cursor-not-allowed"
                : "bg-pink-600 text-white hover:bg-pink-700"
            }`}
          >
            {loading ? "Scheduling..." : "Schedule for Later"}
          </button>

          {message && <ResponseMessage message={message} />}
        </form>
      ) : (
        <div className="space-y-6">
          {/* Pending Items */}
          <div>
            <h3 className="text-lg font-semibold text-gray-800 mb-3">
              Pending ({pendingItems.length})
            </h3>
            <div className="space-y-3">
              {pendingItems.map((item) => (
                <div key={item.id} className="border border-gray-200 rounded-lg p-4">
                  <div className="flex justify-between items-start">
                    <div className="flex-1">
                      <h4 className="font-medium text-gray-900">{item.title}</h4>
                      <p className="text-sm text-gray-600">
                        by {item.authors.join(", ")} • {item.category}
                      </p>
                      <p className="text-sm text-gray-500 mt-1">
                        Scheduled: {new Date(item.scheduledDate).toLocaleString()}
                      </p>
                    </div>
                    <div className="flex gap-2 ml-4">
                      <button
                        onClick={() => handlePublishNow(item)}
                        className="px-3 py-1 bg-green-600 text-white text-sm rounded hover:bg-green-700"
                      >
                        Publish Now
                      </button>
                      <button
                        onClick={() => handleDeleteScheduled(item.id)}
                        className="px-3 py-1 bg-red-600 text-white text-sm rounded hover:bg-red-700"
                      >
                        Delete
                      </button>
                    </div>
                  </div>
                </div>
              ))}
              {pendingItems.length === 0 && (
                <p className="text-gray-500">No pending scheduled items</p>
              )}
            </div>
          </div>

          {/* Published Items */}
          {publishedItems.length > 0 && (
            <div>
              <h3 className="text-lg font-semibold text-green-600 mb-3">
                Published ({publishedItems.length})
              </h3>
              <div className="space-y-2">
                {publishedItems.map((item) => (
                  <div key={item.id} className="border border-green-200 rounded-lg p-3 bg-green-50">
                    <div className="flex justify-between items-center">
                      <div>
                        <h4 className="font-medium text-gray-900">{item.title}</h4>
                        <p className="text-sm text-gray-600">
                          Published • {item.category}
                        </p>
                      </div>
                      <button
                        onClick={() => handleDeleteScheduled(item.id)}
                        className="px-2 py-1 bg-red-500 text-white text-xs rounded hover:bg-red-600"
                      >
                        Remove
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Failed Items */}
          {failedItems.length > 0 && (
            <div>
              <h3 className="text-lg font-semibold text-red-600 mb-3">
                Failed ({failedItems.length})
              </h3>
              <div className="space-y-2">
                {failedItems.map((item) => (
                  <div key={item.id} className="border border-red-200 rounded-lg p-3 bg-red-50">
                    <div className="flex justify-between items-center">
                      <div>
                        <h4 className="font-medium text-gray-900">{item.title}</h4>
                        <p className="text-sm text-red-600">
                          Publish failed • {item.category}
                        </p>
                      </div>
                      <div className="flex gap-2">
                        <button
                          onClick={() => handlePublishNow(item)}
                          className="px-2 py-1 bg-green-500 text-white text-xs rounded hover:bg-green-600"
                        >
                          Retry
                        </button>
                        <button
                          onClick={() => handleDeleteScheduled(item.id)}
                          className="px-2 py-1 bg-red-500 text-white text-xs rounded hover:bg-red-600"
                        >
                          Remove
                        </button>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </section>
  );
}

/* ------------------ UPDATE FORM ------------------ */
function UpdateForm() {
  const availableCategories = DISCOVERY_CATEGORIES;

  const [category, setCategory] = useState<CategoryType>("books");
  const [files, setFiles] = useState<FileItem[]>([]);
  const [selectedFile, setSelectedFile] = useState<FileItem | null>(null);
  const [formData, setFormData] = useState<any>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [showConfirm, setShowConfirm] = useState(false);

  // Load files + titles
  useEffect(() => {
    const fetchFiles = async () => {
      try {
        const res = await fetch(`/api/github/list-files?category=${category}`);
        const filesData: FileItem[] = await res.json();

        const filesWithTitles = await Promise.all(
          filesData.map(async (file) => {
            try {
              const fileRes = await fetch(
                `/api/github/get-file?path=${encodeURIComponent(file.path)}`
              );
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
      }
    };
    fetchFiles();
  }, [category]);

  // Fetch selected file content
  useEffect(() => {
    if (!selectedFile) return;
    fetch(`/api/github/get-file?path=${encodeURIComponent(selectedFile.path)}`)
      .then((res) => res.json())
      .then((data) =>
        setFormData({
          ...data,
          categories: data.categories || [],
        })
      )
      .catch(console.error);
  }, [selectedFile]);

  const handleFieldChange = (field: string, value: string | string[]) => {
    setFormData((prev: any) => ({
      ...prev,
      [field]: value,
    }));
  };

  const toggleCategory = (cat: string) => {
    if (!formData) return;
    const current = formData.categories || [];
    handleFieldChange(
      "categories",
      current.includes(cat)
        ? current.filter((c: string) => c !== cat)
        : [...current, cat]
    );
  };

  const handleUpdate = async () => {
    if (!selectedFile || !formData) return alert("Select a file first");

    setShowConfirm(false);
    setMessage(null);

    try {
      const res = await fetch("/api/github/update", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          category,
          filename: selectedFile.name,
          content: formData,
          sha: selectedFile.sha,
        }),
      });

      const data = await res.json();
      if (res.ok) setMessage("✅ Update successful!");
      else setMessage("❌ Update failed: " + data.error);
    } catch (err: any) {
      setMessage("❌ Error: " + err.message);
    }
  };

  return (
    <section className="bg-white py-16 px-6 max-w-3xl mx-auto rounded-xl shadow-md border space-y-6">
      <h2 className="text-2xl font-bold text-pink-600 mb-4 text-center">
        Update Content
      </h2>

      <CategoryToggle category={category} setCategory={setCategory} />

      {/* File List */}
      <ul className="space-y-2 mb-6">
        {files.map((file) => (
          <li
            key={file.sha}
            onClick={() =>
              setSelectedFile(selectedFile?.name === file.name ? null : file)
            }
            className={`cursor-pointer p-3 rounded-md border transition ${
              selectedFile?.name === file.name
                ? "bg-pink-100 border-pink-400 text-gray-900"
                : "bg-white border-gray-300 hover:bg-gray-100 text-gray-900"
            }`}
          >
            {file.title}
          </li>
        ))}
        {files.length === 0 && <p>No {category} found.</p>}
      </ul>

      {/* Edit Form */}
      {formData && selectedFile && (
        <form className="space-y-4">
          <input
            type="text"
            value={formData.title || ""}
            onChange={(e) => handleFieldChange("title", e.target.value)}
            placeholder="Title"
            className="w-full px-4 py-2 border rounded-md text-gray-800"
          />
          <input
            type="text"
            value={formData.authors?.join(", ") || ""}
            onChange={(e) =>
              handleFieldChange(
                "authors",
                e.target.value.split(",").map((a) => a.trim())
              )
            }
            placeholder="Authors"
            className="w-full px-4 py-2 border rounded-md text-gray-800"
          />

          <div>
            <p className="font-medium mb-2 text-gray-900">Categories:</p>
            <div className="flex flex-wrap gap-2">
              {availableCategories.map((cat) => (
                <label
                  key={cat}
                  onClick={() => toggleCategory(cat)}
                  className={`px-3 py-1 border rounded-md cursor-pointer transition ${
                    formData.categories?.includes(cat)
                      ? "bg-pink-600 text-white border-pink-600"
                      : "bg-gray-200 text-gray-800 border-gray-300 hover:bg-gray-300"
                  }`}
                >
                  {cat}
                </label>
              ))}
            </div>
          </div>

          <textarea
            value={formData.synopsis || ""}
            onChange={(e) => handleFieldChange("synopsis", e.target.value)}
            placeholder="Synopsis"
            className="w-full px-4 py-2 border rounded-md text-gray-800"
          />
          <input
            type="text"
            value={formData.thumbnail || ""}
            onChange={(e) => handleFieldChange("thumbnail", e.target.value)}
            placeholder="Thumbnail URL"
            className="w-full px-4 py-2 border rounded-md text-gray-800"
          />

          <div className="flex gap-3">
            <button
              type="button"
              onClick={() => setShowConfirm(true)}
              className="w-full py-2 bg-pink-600 text-white rounded-md hover:bg-pink-700"
            >
              Update
            </button>
          </div>
        </form>
      )}

      {/* Confirmation Modal */}
      {showConfirm && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-xl max-w-md w-full p-6">
            <h3 className="text-xl font-bold text-pink-600 mb-4">Confirm Update</h3>
            <p className="mb-6">
              Save changes to <strong>{selectedFile?.name}</strong>?
            </p>
            <div className="flex gap-3">
              <button
                onClick={handleUpdate}
                className="w-full py-2 bg-pink-600 text-white rounded-md hover:bg-pink-700"
              >
                Yes, Save
              </button>
              <button
                onClick={() => setShowConfirm(false)}
                className="w-full py-2 bg-gray-300 text-gray-800 rounded-md hover:bg-gray-400"
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}

      {message && <ResponseMessage message={message} />}
    </section>
  );
}

/* ------------------ REMOVE FORM ------------------ */
function RemoveForm() {
  const [category, setCategory] = useState<CategoryType>("books");
  const [files, setFiles] = useState<FileItem[]>([]);
  const [message, setMessage] = useState<string | null>(null);
  const [confirmFile, setConfirmFile] = useState<FileItem | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const fetchFiles = async () => {
      setLoading(true);
      try {
        const res = await fetch(`/api/github/list-files?category=${category}`);
        const filesData: FileItem[] = await res.json();

        const filesWithTitles = await Promise.all(
          filesData.map(async (file) => {
            try {
              const fileRes = await fetch(
                `/api/github/get-file?path=${encodeURIComponent(file.path)}`
              );
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
    fetchFiles();
  }, [category]);

  const handleRemove = async (file: FileItem) => {
    setMessage(null);
    try {
      const res = await fetch("/api/github/remove", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          category,
          filename: file.name,
          sha: file.sha,
        }),
      });
      const data = await res.json();
      if (res.ok) {
        setMessage(`✅ Removed ${file.title || file.name}`);
        setFiles(files.filter((f) => f.name !== file.name));
      } else {
        setMessage("❌ Remove failed: " + data.error);
      }
    } catch (err: any) {
      setMessage("❌ Error: " + err.message);
    } finally {
      setConfirmFile(null);
    }
  };

  return (
    <section className="bg-white py-16 px-6 max-w-3xl mx-auto rounded-xl shadow-md border space-y-6">
      <h2 className="text-2xl font-bold text-pink-600 mb-4 text-center">
        Remove Content
      </h2>

      <CategoryToggle category={category} setCategory={setCategory} />

      {loading ? (
        <p>Loading {category}...</p>
      ) : (
        <ul className="space-y-3">
          {files.map((file) => (
            <li
              key={file.sha}
              className="flex justify-between items-center border border-gray-200 p-3 rounded-md shadow-sm text-gray-800"
            >
              <span>{file.title || file.name}</span>
              <button
                onClick={() => setConfirmFile(file)}
                className="px-3 py-1 bg-red-600 text-white rounded-md hover:bg-red-700"
              >
                Remove
              </button>
            </li>
          ))}
          {files.length === 0 && <p>No {category} found.</p>}
        </ul>
      )}

      {/* Confirmation Modal */}
      {confirmFile && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-xl max-w-md w-full p-6 text-gray-800">
            <h3 className="text-xl font-bold text-pink-600 mb-4">Confirm Remove</h3>
            <p className="mb-6">
              Are you sure you want to remove <strong>{confirmFile.title || confirmFile.name}</strong>?
            </p>
            <div className="flex gap-3">
              <button
                onClick={() => handleRemove(confirmFile)}
                className="w-full py-2 bg-red-600 text-white rounded-md hover:bg-red-700"
              >
                Yes, Remove
              </button>
              <button
                onClick={() => setConfirmFile(null)}
                className="w-full py-2 bg-gray-300 text-gray-800 rounded-md hover:bg-gray-400"
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}

      {message && <ResponseMessage message={message} />}
    </section>
  );
}

/* ------------------ REUSABLE COMPONENTS ------------------ */
function CategoryToggle({
  category,
  setCategory,
}: {
  category: CategoryType;
  setCategory: (cat: CategoryType) => void;
}) {
  return (
    <div className="flex justify-center gap-4 mb-6">
      {(["books", "videos"] as CategoryType[]).map((type) => (
        <button
          key={type}
          onClick={() => setCategory(type)}
          className={`px-4 py-2 rounded-md font-semibold transition ${
            category === type
              ? "bg-pink-600 text-white"
              : "bg-gray-200 text-gray-800 hover:bg-gray-300"
          }`}
        >
          {type.charAt(0).toUpperCase() + type.slice(1)}
        </button>
      ))}
    </div>
  );
}

function CheckboxGroup({
  selected,
  toggle,
}: {
  selected: string[];
  toggle: (cat: string) => void;
}) {
  return (
    <div>
      <p className="font-medium mb-2 text-gray-900">Categories:</p>
      <div className="flex flex-wrap gap-2">
        {DISCOVERY_CATEGORIES.map((cat) => (
          <label
            key={cat}
            onClick={() => toggle(cat)}
            className={`px-3 py-1 border rounded-md cursor-pointer transition ${
              selected.includes(cat)
                ? "bg-pink-600 text-white border-pink-600"
                : "bg-gray-200 text-gray-800 border-gray-300 hover:bg-gray-300"
            }`}
          >
            {cat}
          </label>
        ))}
      </div>
    </div>
  );
}

function ResponseMessage({ message }: { message: string }) {
  return (
    <p
      className={`mt-4 text-center font-medium ${
        message.startsWith("✅") ? "text-green-600" : "text-red-500"
      }`}
    >
      {message}
    </p>
  );
}