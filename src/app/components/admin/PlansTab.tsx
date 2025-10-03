'use client';

import { useState } from 'react';
import { addDoc, collection, updateDoc, doc, deleteDoc } from 'firebase/firestore';
import { db } from '@/lib/firebase';
import { 
  Plus, 
  Edit2, 
  Trash2, 
  Check, 
  X, 
  DollarSign,
  Tag,
  FileText,
  Sparkles,
  Crown,
  Zap
} from 'lucide-react';

interface PlanData {
  id: string;
  name: string;
  price: number;
  description: string;
  features: string[];
  popular?: boolean;
  billingPeriod?: string;
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
    features: "",
    popular: false,
    billingPeriod: "monthly"
  });
  const [editingPlan, setEditingPlan] = useState<PlanData | null>(null);
  const [editPlanForm, setEditPlanForm] = useState({
    name: "",
    price: 0,
    description: "",
    features: "",
    popular: false,
    billingPeriod: "monthly"
  });
  const [showAddForm, setShowAddForm] = useState(false);

  const handleAddPlan = async () => {
    if (!newPlan.name || !newPlan.description || !newPlan.features) {
      alert("Please fill in all fields");
      return;
    }
    
    try {
      await addDoc(collection(db, "plans"), {
        name: newPlan.name,
        price: newPlan.price,
        description: newPlan.description,
        features: newPlan.features.split(",").map(f => f.trim()).filter(f => f),
        popular: newPlan.popular,
        billingPeriod: newPlan.billingPeriod,
        createdAt: new Date()
      });
      setNewPlan({ name: "", price: 0, description: "", features: "", popular: false, billingPeriod: "monthly" });
      setShowAddForm(false);
      fetchPlans();
    } catch (error) {
      console.error("Error adding plan:", error);
      alert("Failed to add plan. Please try again.");
    }
  };

  const handleDeletePlan = async (id: string, name: string) => {
    if (!confirm(`Are you sure you want to delete "${name}" plan? This action cannot be undone.`)) {
      return;
    }
    try {
      await deleteDoc(doc(db, "plans", id));
      fetchPlans();
    } catch (error) {
      console.error("Error deleting plan:", error);
      alert("Failed to delete plan. Please try again.");
    }
  };

  const handleEditPlan = (plan: PlanData) => {
    setEditingPlan(plan);
    setEditPlanForm({
      name: plan.name,
      price: plan.price,
      description: plan.description,
      features: plan.features.join(", "),
      popular: plan.popular || false,
      billingPeriod: plan.billingPeriod || "monthly"
    });
  };

  const handleSavePlan = async () => {
    if (!editingPlan) return;
    if (!editPlanForm.name || !editPlanForm.description || !editPlanForm.features) {
      alert("Please fill in all fields");
      return;
    }

    try {
      await updateDoc(doc(db, "plans", editingPlan.id), {
        name: editPlanForm.name,
        price: editPlanForm.price,
        description: editPlanForm.description,
        features: editPlanForm.features.split(",").map(f => f.trim()).filter(f => f),
        popular: editPlanForm.popular,
        billingPeriod: editPlanForm.billingPeriod,
        updatedAt: new Date()
      });
      setEditingPlan(null);
      fetchPlans();
    } catch (error) {
      console.error("Error updating plan:", error);
      alert("Failed to update plan. Please try again.");
    }
  };

  const getPlanIcon = (name: string) => {
    if (name.toLowerCase().includes('premium') || name.toLowerCase().includes('pro')) {
      return <Crown className="w-6 h-6" />;
    }
    if (name.toLowerCase().includes('enterprise') || name.toLowerCase().includes('business')) {
      return <Sparkles className="w-6 h-6" />;
    }
    return <Zap className="w-6 h-6" />;
  };

  const stats = {
    totalPlans: plans.length,
    averagePrice: plans.length > 0 
      ? (plans.reduce((sum, p) => sum + p.price, 0) / plans.length).toFixed(2)
      : "0",
    popularPlans: plans.filter(p => p.popular).length,
    freePlans: plans.filter(p => p.price === 0).length
  };

  return (
    <section className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-3xl font-bold text-gray-800">Plans Management</h2>
          <p className="text-gray-600 mt-1">Create and manage subscription plans</p>
        </div>
        <button
          onClick={() => setShowAddForm(!showAddForm)}
          className="flex items-center gap-2 px-4 py-2 bg-gradient-to-r from-pink-600 to-purple-600 text-white rounded-lg hover:from-pink-700 hover:to-purple-700 transition-colors font-semibold"
        >
          <Plus className="w-5 h-5" />
          Add New Plan
        </button>
      </div>

      {/* Statistics Cards */}
      <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
        <div className="bg-white rounded-lg shadow p-4 border-l-4 border-blue-500">
          <p className="text-sm text-gray-600">Total Plans</p>
          <p className="text-2xl font-bold text-gray-800">{stats.totalPlans}</p>
        </div>
        <div className="bg-white rounded-lg shadow p-4 border-l-4 border-yellow-500">
          <p className="text-sm text-gray-600">Popular Plans</p>
          <p className="text-2xl font-bold text-yellow-600">{stats.popularPlans}</p>
        </div>
        <div className="bg-white rounded-lg shadow p-4 border-l-4 border-purple-500">
          <p className="text-sm text-gray-600">Free Plans</p>
          <p className="text-2xl font-bold text-purple-600">{stats.freePlans}</p>
        </div>
      </div>

      {/* Add New Plan Form */}
      {showAddForm && (
        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-xl font-bold text-gray-800 mb-4">Create New Plan</h3>
          <div className="space-y-4">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-semibold text-gray-700 mb-2">
                  <Tag className="w-4 h-4 inline mr-1" />
                  Plan Name
                </label>
                <input
                  value={newPlan.name}
                  onChange={e => setNewPlan({ ...newPlan, name: e.target.value })}
                  placeholder="e.g., Premium Plan"
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
                />
              </div>
              
              <div>
                <label className="block text-sm font-semibold text-gray-700 mb-2">
                  <DollarSign className="w-4 h-4 inline mr-1" />
                  Price
                </label>
                <input
                  type="number"
                  step="0.01"
                  value={newPlan.price}
                  onChange={e => setNewPlan({ ...newPlan, price: Number(e.target.value) })}
                  placeholder="0.00"
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
                />
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-semibold text-gray-700 mb-2">
                  Billing Period
                </label>
                <select
                  value={newPlan.billingPeriod}
                  onChange={e => setNewPlan({ ...newPlan, billingPeriod: e.target.value })}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
                >
                  <option value="monthly">Monthly</option>
                  <option value="yearly">Yearly</option>
                  <option value="lifetime">Lifetime</option>
                </select>
              </div>

              <div className="flex items-end">
                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={newPlan.popular}
                    onChange={e => setNewPlan({ ...newPlan, popular: e.target.checked })}
                    className="w-5 h-5 text-pink-600 rounded focus:ring-2 focus:ring-pink-500"
                  />
                  <span className="text-sm font-semibold text-gray-700">Mark as Popular</span>
                </label>
              </div>
            </div>

            <div>
              <label className="block text-sm font-semibold text-gray-700 mb-2">
                <FileText className="w-4 h-4 inline mr-1" />
                Description
              </label>
              <textarea
                value={newPlan.description}
                onChange={e => setNewPlan({ ...newPlan, description: e.target.value })}
                placeholder="Brief description of the plan"
                rows={3}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
              />
            </div>

            <div>
              <label className="block text-sm font-semibold text-gray-700 mb-2">
                <Check className="w-4 h-4 inline mr-1" />
                Features (comma-separated)
              </label>
              <textarea
                value={newPlan.features}
                onChange={e => setNewPlan({ ...newPlan, features: e.target.value })}
                placeholder="Feature 1, Feature 2, Feature 3"
                rows={3}
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
              />
              <p className="text-xs text-gray-500 mt-1">Separate each feature with a comma</p>
            </div>

            <div className="flex justify-end gap-3">
              <button
                onClick={() => {
                  setShowAddForm(false);
                  setNewPlan({ name: "", price: 0, description: "", features: "", popular: false, billingPeriod: "monthly" });
                }}
                className="px-6 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300 transition-colors font-semibold"
              >
                Cancel
              </button>
              <button
                onClick={handleAddPlan}
                className="px-6 py-2 bg-gradient-to-r from-pink-600 to-purple-600 text-white rounded-lg hover:from-pink-700 hover:to-purple-700 transition-colors font-semibold"
              >
                Create Plan
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Plans Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {plans.length === 0 ? (
          <div className="col-span-full bg-white rounded-lg shadow p-12 text-center">
            <Tag className="w-16 h-16 text-gray-300 mx-auto mb-4" />
            <p className="text-gray-500 text-lg">No plans created yet</p>
            <p className="text-gray-400 text-sm mt-2">Click "Add New Plan" to create your first plan</p>
          </div>
        ) : (
          plans.map(p => (
            <div
              key={p.id}
              className={`bg-white rounded-xl shadow-lg hover:shadow-xl transition-all p-6 relative overflow-hidden ${
                p.popular ? 'ring-2 ring-pink-500' : ''
              }`}
            >
              {p.popular && (
                <div className="absolute top-0 right-0 bg-gradient-to-r from-pink-500 to-purple-500 text-white px-4 py-1 text-xs font-bold rounded-bl-lg">
                  POPULAR
                </div>
              )}

              <div className="flex items-center gap-3 mb-4">
                <div className="p-3 bg-gradient-to-br from-pink-100 to-purple-100 rounded-lg text-pink-600">
                  {getPlanIcon(p.name)}
                </div>
                <div>
                  <h3 className="text-xl font-bold text-gray-800">{p.name}</h3>
                  <p className="text-sm text-gray-500 capitalize">{p.billingPeriod || 'monthly'}</p>
                </div>
              </div>

              <div className="mb-4">
                <div className="flex items-baseline gap-1">
                  <span className="text-4xl font-bold text-gray-800">
                    ${p.price}
                  </span>
                  {p.price > 0 && (
                    <span className="text-gray-500">/{p.billingPeriod === 'yearly' ? 'year' : 'month'}</span>
                  )}
                </div>
              </div>

              <p className="text-gray-600 mb-4 min-h-[48px]">
                {p.description}
              </p>

              <div className="mb-6">
                <p className="text-sm font-semibold text-gray-700 mb-2">Features:</p>
                <ul className="space-y-2">
                  {p.features.map((f, i) => (
                    <li key={i} className="flex items-start gap-2 text-sm text-gray-600">
                      <Check className="w-4 h-4 text-green-500 flex-shrink-0 mt-0.5" />
                      <span>{f}</span>
                    </li>
                  ))}
                </ul>
              </div>

              <div className="flex gap-2 pt-4 border-t">
                <button
                  onClick={() => handleEditPlan(p)}
                  className="flex-1 flex items-center justify-center gap-2 px-4 py-2 bg-blue-100 text-blue-700 rounded-lg hover:bg-blue-200 transition-colors font-semibold"
                >
                  <Edit2 className="w-4 h-4" />
                  Edit
                </button>
                <button
                  onClick={() => handleDeletePlan(p.id, p.name)}
                  className="flex-1 flex items-center justify-center gap-2 px-4 py-2 bg-red-100 text-red-700 rounded-lg hover:bg-red-200 transition-colors font-semibold"
                >
                  <Trash2 className="w-4 h-4" />
                  Delete
                </button>
              </div>
            </div>
          ))
        )}
      </div>

      {/* Edit Plan Modal */}
      {editingPlan && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-2xl transform transition-all max-h-[90vh] overflow-y-auto">
            <div className="bg-gradient-to-r from-pink-600 to-purple-600 text-white p-6 rounded-t-2xl sticky top-0">
              <div className="flex justify-between items-center">
                <h3 className="text-2xl font-bold">Edit Plan</h3>
                <button
                  onClick={() => setEditingPlan(null)}
                  className="p-1 hover:bg-white hover:bg-opacity-20 rounded-lg transition-colors"
                >
                  <X className="w-6 h-6" />
                </button>
              </div>
            </div>

            <div className="p-6 space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-semibold text-gray-700 mb-2">
                    Plan Name
                  </label>
                  <input
                    type="text"
                    value={editPlanForm.name}
                    onChange={(e) => setEditPlanForm({ ...editPlanForm, name: e.target.value })}
                    placeholder="Plan Name"
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
                  />
                </div>

                <div>
                  <label className="block text-sm font-semibold text-gray-700 mb-2">
                    Price
                  </label>
                  <input
                    type="number"
                    step="0.01"
                    value={editPlanForm.price}
                    onChange={(e) => setEditPlanForm({ ...editPlanForm, price: Number(e.target.value) })}
                    placeholder="Price"
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
                  />
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-semibold text-gray-700 mb-2">
                    Billing Period
                  </label>
                  <select
                    value={editPlanForm.billingPeriod}
                    onChange={(e) => setEditPlanForm({ ...editPlanForm, billingPeriod: e.target.value })}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
                  >
                    <option value="monthly">Monthly</option>
                    <option value="yearly">Yearly</option>
                    <option value="lifetime">Lifetime</option>
                  </select>
                </div>

                <div className="flex items-end">
                  <label className="flex items-center gap-2 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={editPlanForm.popular}
                      onChange={(e) => setEditPlanForm({ ...editPlanForm, popular: e.target.checked })}
                      className="w-5 h-5 text-pink-600 rounded focus:ring-2 focus:ring-pink-500"
                    />
                    <span className="text-sm font-semibold text-gray-700">Mark as Popular</span>
                  </label>
                </div>
              </div>

              <div>
                <label className="block text-sm font-semibold text-gray-700 mb-2">
                  Description
                </label>
                <textarea
                  value={editPlanForm.description}
                  onChange={(e) => setEditPlanForm({ ...editPlanForm, description: e.target.value })}
                  placeholder="Description"
                  rows={3}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
                />
              </div>

              <div>
                <label className="block text-sm font-semibold text-gray-700 mb-2">
                  Features (comma-separated)
                </label>
                <textarea
                  value={editPlanForm.features}
                  onChange={(e) => setEditPlanForm({ ...editPlanForm, features: e.target.value })}
                  placeholder="Features (comma-separated)"
                  rows={4}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
                />
              </div>
            </div>

            <div className="flex justify-end gap-3 px-6 pb-6">
              <button
                onClick={() => setEditingPlan(null)}
                className="px-6 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300 transition-colors font-semibold"
              >
                Cancel
              </button>
              <button
                onClick={handleSavePlan}
                className="px-6 py-2 bg-gradient-to-r from-pink-600 to-purple-600 text-white rounded-lg hover:from-pink-700 hover:to-purple-700 transition-colors font-semibold"
              >
                Save Changes
              </button>
            </div>
          </div>
        </div>
      )}
    </section>
  );
}