'use client';

import { useState } from 'react';
import { addDoc, collection, updateDoc, doc, deleteDoc } from 'firebase/firestore';
import { db } from '@/lib/firebase';

interface PlanData {
  id: string;
  name: string;
  price: number;
  description: string;
  features: string[];
}

interface PlansTabProps {
  plans: PlanData[];
  fetchPlans: () => void;
}

export default function PlansTab({ plans, fetchPlans }: PlansTabProps) {
  const [newPlan, setNewPlan] = useState({
    name: "",
    price: 0,
    description: "",
    features: ""
  });
  const [editingPlan, setEditingPlan] = useState<PlanData | null>(null);
  const [editPlanForm, setEditPlanForm] = useState({
    name: "",
    price: 0,
    description: "",
    features: ""
  });

  const handleAddPlan = async (e: React.FormEvent) => {
    e.preventDefault();
    await addDoc(collection(db, "plans"), {
      name: newPlan.name,
      price: newPlan.price,
      description: newPlan.description,
      features: newPlan.features.split(",").map(f => f.trim())
    });
    setNewPlan({ name: "", price: 0, description: "", features: "" });
    fetchPlans();
  };

  const handleDeletePlan = async (id: string) => {
    if (!confirm("Are you sure you want to delete this plan? This action cannot be undone.")) {
      return;
    }
    await deleteDoc(doc(db, "plans", id));
    fetchPlans();
  };

  const handleEditPlan = (plan: PlanData) => {
    setEditingPlan(plan);
    setEditPlanForm({
      name: plan.name,
      price: plan.price,
      description: plan.description,
      features: plan.features.join(", ")
    });
  };

  const handleSavePlan = async () => {
    if (!editingPlan) return;
    await updateDoc(doc(db, "plans", editingPlan.id), {
      name: editPlanForm.name,
      price: editPlanForm.price,
      description: editPlanForm.description,
      features: editPlanForm.features.split(",").map(f => f.trim())
    });
    setEditingPlan(null);
    fetchPlans();
  };

  return (
    <section>
      <h2 className="text-2xl font-bold text-pink-600 mb-6">Plans</h2>
      
      {/* Add New Plan Form */}
      <form onSubmit={handleAddPlan} className="space-y-2 mb-6">
        <input
          value={newPlan.name}
          onChange={e => setNewPlan({ ...newPlan, name: e.target.value })}
          placeholder="Name"
          className="border p-2 w-full"
        />
        <input
          type="number"
          value={newPlan.price}
          onChange={e => setNewPlan({ ...newPlan, price: Number(e.target.value) })}
          placeholder="Price"
          className="border p-2 w-full"
        />
        <textarea
          value={newPlan.description}
          onChange={e => setNewPlan({ ...newPlan, description: e.target.value })}
          placeholder="Description"
          className="border p-2 w-full"
        />
        <input
          value={newPlan.features}
          onChange={e => setNewPlan({ ...newPlan, features: e.target.value })}
          placeholder="Features (comma-separated)"
          className="border p-2 w-full"
        />
        <button type="submit" className="px-3 py-1 bg-pink-600 text-white rounded">
          Add Plan
        </button>
      </form>

      {/* Plans List */}
      <ul className="space-y-4">
        {plans.map(p => (
          <li key={p.id} className="p-4 border rounded flex justify-between items-center">
            <div>
              <p className="font-bold">{p.name} - ${p.price}</p>
              <p>{p.description}</p>
              <ul className="list-disc pl-5 text-sm text-gray-600">
                {p.features.map((f, i) => <li key={i}>{f}</li>)}
              </ul>
            </div>
            <div className="flex gap-2">
              <button
                onClick={() => handleDeletePlan(p.id)}
                className="px-3 py-1 bg-red-500 text-white rounded"
              >
                Delete
              </button>
              <button
                onClick={() => handleEditPlan(p)}
                className="px-3 py-1 bg-yellow-500 text-white rounded"
              >
                Edit
              </button>
            </div>
          </li>
        ))}
      </ul>

      {/* Edit Plan Modal */}
      {editingPlan && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white p-6 rounded-lg shadow-lg w-full max-w-md">
            <h3 className="text-xl font-bold mb-4">Edit Plan</h3>
            <div className="space-y-3">
              <input
                type="text"
                value={editPlanForm.name}
                onChange={(e) => setEditPlanForm({ ...editPlanForm, name: e.target.value })}
                placeholder="Plan Name"
                className="w-full border p-2 rounded"
              />
              <input
                type="number"
                value={editPlanForm.price}
                onChange={(e) => setEditPlanForm({ ...editPlanForm, price: Number(e.target.value) })}
                placeholder="Price"
                className="w-full border p-2 rounded"
              />
              <textarea
                value={editPlanForm.description}
                onChange={(e) => setEditPlanForm({ ...editPlanForm, description: e.target.value })}
                placeholder="Description"
                className="w-full border p-2 rounded"
              />
              <input
                type="text"
                value={editPlanForm.features}
                onChange={(e) => setEditPlanForm({ ...editPlanForm, features: e.target.value })}
                placeholder="Features (comma-separated)"
                className="w-full border p-2 rounded"
              />
            </div>
            <div className="flex justify-end gap-2 mt-6">
              <button
                onClick={() => setEditingPlan(null)}
                className="px-4 py-2 bg-gray-300 rounded"
              >
                Cancel
              </button>
              <button
                onClick={handleSavePlan}
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