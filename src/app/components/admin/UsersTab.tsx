'use client';

import { useState } from 'react';
import { updateDoc, doc, deleteDoc } from 'firebase/firestore';
import { db } from '@/lib/firebase';

interface UserData {
  uid: string;
  fullName: string;
  email: string;
  role?: string;
  plan?: string;
}

interface UsersTabProps {
  users: UserData[];
  setUsers: React.Dispatch<React.SetStateAction<UserData[]>>;
}

export default function UsersTab({ users, setUsers }: UsersTabProps) {
  const [editingUser, setEditingUser] = useState<UserData | null>(null);
  const [editForm, setEditForm] = useState({
    fullName: "",
    email: "",
    role: "",
    plan: ""
  });

  const handleDeleteUser = async (uid: string) => {
    if (!confirm("Are you sure you want to delete this user? This action cannot be undone.")) {
      return;
    }
    await deleteDoc(doc(db, "users", uid));
    setUsers(prev => prev.filter(u => u.uid !== uid));
  };

  const handleToggleUser = async (uid: string, role?: string) => {
    const newRole = role === "inactive" ? "user" : "inactive";
    if (!confirm(`Are you sure you want to ${newRole === "inactive" ? "deactivate" : "activate"} this account?`)) {
      return;
    }
    await updateDoc(doc(db, "users", uid), { role: newRole });
    setUsers(prev => prev.map(u => (u.uid === uid ? { ...u, role: newRole } : u)));
  };

  const handleEditUser = (user: UserData) => {
    setEditingUser(user);
    setEditForm({
      fullName: user.fullName,
      email: user.email,
      role: user.role || "user",
      plan: user.plan || "Free Plans"
    });
  };

  const handleSaveUser = async () => {
    if (!editingUser) return;
    await updateDoc(doc(db, "users", editingUser.uid), editForm);
    setUsers(prev => prev.map(u => u.uid === editingUser.uid ? { ...u, ...editForm } : u));
    setEditingUser(null);
  };

  return (
    <section>
      <h2 className="text-2xl font-bold text-pink-600 mb-6">Users</h2>
      <table className="w-full border">
        <thead>
          <tr className="bg-gray-100">
            <th className="p-2">Name</th>
            <th className="p-2">Email</th>
            <th className="p-2">Role</th>
            <th className="p-2">Plan</th>
            <th className="p-2">Actions</th>
          </tr>
        </thead>
        <tbody>
          {users.map(u => (
            <tr key={u.uid} className="border-t">
              <td className="p-2">{u.fullName}</td>
              <td className="p-2">{u.email}</td>
              <td className="p-2">{u.role}</td>
              <td className="p-2">{u.plan || "—"}</td>
              <td className="p-2 flex gap-2">
                <button
                  onClick={() => handleToggleUser(u.uid, u.role)}
                  className="px-2 py-1 bg-blue-500 text-white rounded"
                >
                  {u.role === "inactive" ? "Activate" : "Deactivate"}
                </button>
                <button
                  onClick={() => handleDeleteUser(u.uid)}
                  className="px-2 py-1 bg-red-500 text-white rounded"
                >
                  Delete
                </button>
                <button
                  onClick={() => handleEditUser(u)}
                  className="px-2 py-1 bg-yellow-500 text-white rounded"
                >
                  Edit
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      {/* Edit User Modal */}
      {editingUser && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white p-6 rounded-lg shadow-lg w-full max-w-md">
            <h3 className="text-xl font-bold mb-4">Edit User</h3>
            <div className="space-y-3">
              <input
                type="text"
                value={editForm.fullName}
                onChange={(e) => setEditForm({ ...editForm, fullName: e.target.value })}
                placeholder="Full Name"
                className="w-full border p-2 rounded"
              />
              <input
                type="email"
                value={editForm.email}
                onChange={(e) => setEditForm({ ...editForm, email: e.target.value })}
                placeholder="Email"
                className="w-full border p-2 rounded"
              />
              <select
                value={editForm.role}
                onChange={(e) => setEditForm({ ...editForm, role: e.target.value })}
                className="w-full border p-2 rounded"
              >
                <option value="user">User</option>
                <option value="admin">Admin</option>
                <option value="inactive">Inactive</option>
              </select>
              <select
                value={editForm.plan}
                onChange={(e) => setEditForm({ ...editForm, plan: e.target.value })}
                className="w-full border p-2 rounded"
              >
                <option value="Free Plans">Free Plans</option>
                <option value="Basic">Basic</option>
                <option value="Premium">Premium</option>
              </select>
            </div>
            <div className="flex justify-end gap-2 mt-6">
              <button
                onClick={() => setEditingUser(null)}
                className="px-4 py-2 bg-gray-300 rounded"
              >
                Cancel
              </button>
              <button
                onClick={handleSaveUser}
                className="px-4 py-2 bg-pink-600 text-white rounded"
              >
                Save
              </button>
            </div>
          </div>
        </div>
      )}
    </section>
  );
}