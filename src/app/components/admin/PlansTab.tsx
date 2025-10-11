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
  Zap,
  Save,
  AlertCircle
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
  const [showAddModal, setShowAddModal] = useState(false);
  const [showDeleteModal, setShowDeleteModal] = useState(false);
  const [planToDelete, setPlanToDelete] = useState<{id: string, name: string} | null>(null);
  const [deleteConfirmText, setDeleteConfirmText] = useState("");
  const [deletingPlanId, setDeletingPlanId] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [showSuccessMessage, setShowSuccessMessage] = useState(false);
  const [showDeleteMessage, setShowDeleteMessage] = useState(false);
  const [validationErrors, setValidationErrors] = useState<{[key: string]: string}>({});

  const validatePlanForm = (formData: typeof newPlan) => {
    const errors: {[key: string]: string} = {};

    if (!formData.name.trim()) {
      errors.name = "Plan name is required";
    } else if (formData.name.trim().length < 3) {
      errors.name = "Plan name must be at least 3 characters";
    }

    if (!formData.description.trim()) {
      errors.description = "Description is required";
    } else if (formData.description.trim().length < 10) {
      errors.description = "Description must be at least 10 characters";
    }

    if (formData.price < 0) {
      errors.price = "Price cannot be negative";
    }

    if (!formData.features.trim()) {
      errors.features = "At least one feature is required";
    } else {
      const featuresArray = formData.features.split(",").map(f => f.trim()).filter(f => f);
      if (featuresArray.length === 0) {
        errors.features = "At least one feature is required";
      }
    }

    return errors;
  };

  const handleAddPlan = async () => {
    // Validate form
    const errors = validatePlanForm(newPlan);
    if (Object.keys(errors).length > 0) {
      setValidationErrors(errors);
      return;
    }

    setIsSubmitting(true);
    setValidationErrors({});
    
    try {
      await addDoc(collection(db, "plans"), {
        name: newPlan.name.trim(),
        price: newPlan.price,
        description: newPlan.description.trim(),
        features: newPlan.features.split(",").map(f => f.trim()).filter(f => f),
        popular: newPlan.popular,
        billingPeriod: newPlan.billingPeriod,
        createdAt: new Date()
      });
      
      // Reset form and close modal
      setNewPlan({ name: "", price: 0, description: "", features: "", popular: false, billingPeriod: "monthly" });
      setShowAddModal(false);
      
      // Show success message
      setShowSuccessMessage(true);
      setTimeout(() => setShowSuccessMessage(false), 3000);
      
      fetchPlans();
    } catch (error) {
      console.error("Error adding plan:", error);
      alert("Failed to add plan. Please try again.");
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleDeletePlan = (id: string, name: string) => {
    setPlanToDelete({ id, name });
    setShowDeleteModal(true);
    setDeleteConfirmText("");
  };

  const confirmDeletePlan = async () => {
    if (!planToDelete) return;
    
    if (deleteConfirmText !== "DELETE") {
      alert("Please type DELETE to confirm deletion");
      return;
    }

    setDeletingPlanId(planToDelete.id);
    
    try {
      await deleteDoc(doc(db, "plans", planToDelete.id));
      fetchPlans();
      
      // Show success message
      setShowDeleteMessage(true);
      setTimeout(() => setShowDeleteMessage(false), 3000);
      
      // Close modal and reset
      setShowDeleteModal(false);
      setPlanToDelete(null);
      setDeleteConfirmText("");
    } catch (error) {
      console.error("Error deleting plan:", error);
      alert("Failed to delete plan. Please try again.");
    } finally {
      setDeletingPlanId(null);
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
    setValidationErrors({});
  };

  const handleCancelEdit = () => {
    if (!editingPlan) return;

    const hasChanges = 
      editPlanForm.name !== editingPlan.name ||
      editPlanForm.price !== editingPlan.price ||
      editPlanForm.description !== editingPlan.description ||
      editPlanForm.features !== editingPlan.features.join(", ") ||
      editPlanForm.popular !== (editingPlan.popular || false) ||
      editPlanForm.billingPeriod !== (editingPlan.billingPeriod || "monthly");

    if (hasChanges) {
      if (confirm("You have unsaved changes. Are you sure you want to cancel?")) {
        setEditingPlan(null);
        setValidationErrors({});
      }
    } else {
      setEditingPlan(null);
      setValidationErrors({});
    }
  };

  const handleCancelAdd = () => {
    const hasChanges = 
      newPlan.name.trim() !== "" ||
      newPlan.price !== 0 ||
      newPlan.description.trim() !== "" ||
      newPlan.features.trim() !== "" ||
      newPlan.popular !== false ||
      newPlan.billingPeriod !== "monthly";

    if (hasChanges) {
      if (confirm("You have unsaved changes. Are you sure you want to cancel?")) {
        setShowAddModal(false);
        setValidationErrors({});
        setNewPlan({ name: "", price: 0, description: "", features: "", popular: false, billingPeriod: "monthly" });
      }
    } else {
      setShowAddModal(false);
      setValidationErrors({});
    }
  };

  const handleSavePlan = async () => {
    if (!editingPlan) return;
    
    // Validate form
    const errors = validatePlanForm(editPlanForm);
    if (Object.keys(errors).length > 0) {
      setValidationErrors(errors);
      return;
    }

    setIsSubmitting(true);
    setValidationErrors({});

    try {
      await updateDoc(doc(db, "plans", editingPlan.id), {
        name: editPlanForm.name.trim(),
        price: editPlanForm.price,
        description: editPlanForm.description.trim(),
        features: editPlanForm.features.split(",").map(f => f.trim()).filter(f => f),
        popular: editPlanForm.popular,
        billingPeriod: editPlanForm.billingPeriod,
        updatedAt: new Date()
      });
      
      setEditingPlan(null);
      
      // Show success message
      setShowSuccessMessage(true);
      setTimeout(() => setShowSuccessMessage(false), 3000);
      
      fetchPlans();
    } catch (error) {
      console.error("Error updating plan:", error);
      alert("Failed to update plan. Please try again.");
    } finally {
      setIsSubmitting(false);
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
      {/* Success Message */}
      {showSuccessMessage && (
        <div className="fixed top-4 right-4 z-50 bg-green-500 text-white px-6 py-4 rounded-lg shadow-lg flex items-center gap-3 animate-fade-in">
          <div className="w-6 h-6 bg-white rounded-full flex items-center justify-center">
            <svg className="w-4 h-4 text-green-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
            </svg>
          </div>
          <span className="font-semibold">Plan saved successfully!</span>
        </div>
      )}

      {/* Delete Success Message */}
      {showDeleteMessage && (
        <div className="fixed top-4 right-4 z-50 bg-red-500 text-white px-6 py-4 rounded-lg shadow-lg flex items-center gap-3 animate-fade-in">
          <div className="w-6 h-6 bg-white rounded-full flex items-center justify-center">
            <svg className="w-4 h-4 text-red-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
            </svg>
          </div>
          <span className="font-semibold">Plan deleted successfully!</span>
        </div>
      )}

      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-3xl font-bold text-gray-800">Plans Management</h2>
          <p className="text-gray-600 mt-1">Create and manage subscription plans</p>
        </div>
        <button
          onClick={() => {
            setShowAddModal(true);
            setValidationErrors({});
            setNewPlan({ name: "", price: 0, description: "", features: "", popular: false, billingPeriod: "monthly" });
          }}
          className="flex items-center gap-2 px-4 py-2 bg-gradient-to-r from-pink-600 to-purple-600 text-white rounded-lg hover:from-pink-700 hover:to-purple-700 transition-colors font-semibold"
        >
          <Plus className="w-5 h-5" />
          Add New Plan
        </button>
      </div>

      {/* Statistics Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div className="bg-white rounded-lg shadow p-4 border-l-4 border-blue-500">
          <p className="text-sm text-gray-600">Total Plans</p>
          <p className="text-2xl font-bold text-gray-800">{stats.totalPlans}</p>
        </div>
        <div className="bg-white rounded-lg shadow p-4 border-l-4 border-green-500">
          <p className="text-sm text-gray-600">Average Price</p>
          <p className="text-2xl font-bold text-green-600">${stats.averagePrice}</p>
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
                    <span className="text-gray-500">/{p.billingPeriod === 'yearly' ? 'year' : p.billingPeriod === 'lifetime' ? 'lifetime' : 'month'}</span>
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
                  disabled={deletingPlanId === p.id}
                  className="flex-1 flex items-center justify-center gap-2 px-4 py-2 bg-red-100 text-red-700 rounded-lg hover:bg-red-200 transition-colors font-semibold disabled:opacity-50"
                >
                  {deletingPlanId === p.id ? (
                    <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-red-700"></div>
                  ) : (
                    <Trash2 className="w-4 h-4" />
                  )}
                  Delete
                </button>
              </div>
            </div>
          ))
        )}
      </div>

      {/* Add Plan Modal */}
      {showAddModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-2xl transform transition-all max-h-[90vh] overflow-y-auto">
            <div className="bg-gradient-to-r from-pink-600 to-purple-600 text-white p-6 rounded-t-2xl sticky top-0">
              <div className="flex justify-between items-center">
                <div>
                  <h3 className="text-2xl font-bold">Create New Plan</h3>
                  <p className="text-pink-100 text-sm mt-1">Add a new subscription plan</p>
                </div>
                <button
                  onClick={handleCancelAdd}
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
                    <Tag className="w-4 h-4 inline mr-1" />
                    Plan Name *
                  </label>
                  <input
                    value={newPlan.name}
                    onChange={e => {
                      setNewPlan({ ...newPlan, name: e.target.value });
                      if (validationErrors.name) {
                        setValidationErrors({...validationErrors, name: ""});
                      }
                    }}
                    placeholder="e.g., Premium Plan"
                    className={`w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent ${
                      validationErrors.name ? 'border-red-500' : 'border-gray-300'
                    }`}
                  />
                  {validationErrors.name && (
                    <p className="text-red-500 text-xs mt-1 flex items-center gap-1">
                      <AlertCircle className="w-3 h-3" />
                      {validationErrors.name}
                    </p>
                  )}
                </div>
                
                <div>
                  <label className="block text-sm font-semibold text-gray-700 mb-2">
                    <DollarSign className="w-4 h-4 inline mr-1" />
                    Price *
                  </label>
                  <input
                    type="number"
                    step="0.01"
                    min="0"
                    value={newPlan.price}
                    onChange={e => {
                      setNewPlan({ ...newPlan, price: Number(e.target.value) });
                      if (validationErrors.price) {
                        setValidationErrors({...validationErrors, price: ""});
                      }
                    }}
                    placeholder="0.00"
                    className={`w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent ${
                      validationErrors.price ? 'border-red-500' : 'border-gray-300'
                    }`}
                  />
                  {validationErrors.price && (
                    <p className="text-red-500 text-xs mt-1 flex items-center gap-1">
                      <AlertCircle className="w-3 h-3" />
                      {validationErrors.price}
                    </p>
                  )}
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
                  <label className="flex items-center gap-2 px-4 py-2 bg-pink-50 border border-pink-200 rounded-lg cursor-pointer hover:bg-pink-100 transition-colors">
                    <input
                      type="checkbox"
                      checked={newPlan.popular}
                      onChange={e => setNewPlan({ ...newPlan, popular: e.target.checked })}
                      className="w-5 h-5 text-pink-600 rounded focus:ring-2 focus:ring-pink-500"
                    />
                    <span className="text-sm font-semibold text-gray-700 flex items-center gap-1">
                      <Sparkles className="w-4 h-4 text-pink-600" />
                      Mark as Popular
                    </span>
                  </label>
                </div>
              </div>

              <div>
                <label className="block text-sm font-semibold text-gray-700 mb-2">
                  <FileText className="w-4 h-4 inline mr-1" />
                  Description *
                </label>
                <textarea
                  value={newPlan.description}
                  onChange={e => {
                    setNewPlan({ ...newPlan, description: e.target.value });
                    if (validationErrors.description) {
                      setValidationErrors({...validationErrors, description: ""});
                    }
                  }}
                  placeholder="Brief description of the plan"
                  rows={3}
                  className={`w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent ${
                    validationErrors.description ? 'border-red-500' : 'border-gray-300'
                  }`}
                />
                {validationErrors.description && (
                  <p className="text-red-500 text-xs mt-1 flex items-center gap-1">
                    <AlertCircle className="w-3 h-3" />
                    {validationErrors.description}
                  </p>
                )}
              </div>

              <div>
                <label className="block text-sm font-semibold text-gray-700 mb-2">
                  <Check className="w-4 h-4 inline mr-1" />
                  Features (comma-separated) *
                </label>
                <textarea
                  value={newPlan.features}
                  onChange={e => {
                    setNewPlan({ ...newPlan, features: e.target.value });
                    if (validationErrors.features) {
                      setValidationErrors({...validationErrors, features: ""});
                    }
                  }}
                  placeholder="Feature 1, Feature 2, Feature 3"
                  rows={4}
                  className={`w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent ${
                    validationErrors.features ? 'border-red-500' : 'border-gray-300'
                  }`}
                />
                {validationErrors.features ? (
                  <p className="text-red-500 text-xs mt-1 flex items-center gap-1">
                    <AlertCircle className="w-3 h-3" />
                    {validationErrors.features}
                  </p>
                ) : (
                  <p className="text-xs text-gray-500 mt-1">Separate each feature with a comma</p>
                )}
              </div>

              {newPlan.features && !validationErrors.features && (
                <div className="bg-blue-50 p-3 rounded-lg border border-blue-200">
                  <p className="text-xs font-semibold text-blue-800 mb-2">Preview Features:</p>
                  <ul className="space-y-1">
                    {newPlan.features.split(",").map(f => f.trim()).filter(f => f).map((f, i) => (
                      <li key={i} className="flex items-start gap-2 text-xs text-blue-700">
                        <Check className="w-3 h-3 text-blue-500 flex-shrink-0 mt-0.5" />
                        <span>{f}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>

            <div className="flex justify-end gap-3 px-6 pb-6 border-t pt-4">
              <button
                onClick={handleCancelAdd}
                className="px-6 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300 transition-colors font-semibold"
                disabled={isSubmitting}
              >
                Cancel
              </button>
              <button
                onClick={handleAddPlan}
                disabled={isSubmitting}
                className="flex items-center gap-2 px-6 py-2 bg-gradient-to-r from-pink-600 to-purple-600 text-white rounded-lg hover:from-pink-700 hover:to-purple-700 transition-colors font-semibold disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {isSubmitting ? (
                  <>
                    <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white"></div>
                    Creating...
                  </>
                ) : (
                  <>
                    <Save className="w-4 h-4" />
                    Create Plan
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Delete Confirmation Modal */}
      {showDeleteModal && planToDelete && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-md transform transition-all">
            <div className="bg-gradient-to-r from-red-600 to-red-700 text-white p-6 rounded-t-2xl">
              <div className="flex items-center gap-3">
                <div className="w-12 h-12 bg-white bg-opacity-20 rounded-full flex items-center justify-center">
                  <Trash2 className="w-6 h-6" />
                </div>
                <div>
                  <h3 className="text-2xl font-bold">Delete Plan</h3>
                  <p className="text-red-100 text-sm">This action cannot be undone</p>
                </div>
              </div>
            </div>

            <div className="p-6 space-y-4">
              <div className="bg-red-50 border-l-4 border-red-500 p-4 rounded">
                <div className="flex items-start gap-3">
                  <svg className="w-5 h-5 text-red-500 mt-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                  </svg>
                  <div>
                    <p className="font-semibold text-red-800">Warning: Permanent Deletion</p>
                    <p className="text-sm text-red-700 mt-1">
                      You are about to delete the <span className="font-bold">"{planToDelete.name}"</span> plan. 
                      This will permanently remove this plan and may affect users with active subscriptions.
                    </p>
                  </div>
                </div>
              </div>

              <div>
                <label className="block text-sm font-semibold text-gray-700 mb-2">
                  Type <span className="text-red-600 font-bold">DELETE</span> to confirm
                </label>
                <input
                  type="text"
                  value={deleteConfirmText}
                  onChange={(e) => setDeleteConfirmText(e.target.value)}
                  placeholder="Type DELETE here"
                  className="w-full px-4 py-2 border-2 border-gray-300 rounded-lg focus:ring-2 focus:ring-red-500 focus:border-red-500"
                  autoFocus
                />
                <p className="text-xs text-gray-500 mt-2">
                  This confirmation helps prevent accidental deletions
                </p>
              </div>
            </div>

            <div className="flex justify-end gap-3 px-6 pb-6">
              <button
                onClick={() => {
                  setShowDeleteModal(false);
                  setPlanToDelete(null);
                  setDeleteConfirmText("");
                }}
                className="px-6 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300 transition-colors font-semibold"
              >
                Cancel
              </button>
              <button
                onClick={confirmDeletePlan}
                disabled={deleteConfirmText !== "DELETE" || deletingPlanId !== null}
                className="flex items-center gap-2 px-6 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors font-semibold disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {deletingPlanId ? (
                  <>
                    <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white"></div>
                    Deleting...
                  </>
                ) : (
                  <>
                    <Trash2 className="w-4 h-4" />
                    Delete Plan
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Edit Plan Modal */}
      {editingPlan && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-2xl transform transition-all max-h-[90vh] overflow-y-auto">
            <div className="bg-gradient-to-r from-pink-600 to-purple-600 text-white p-6 rounded-t-2xl sticky top-0">
              <div className="flex justify-between items-center">
                <div>
                  <h3 className="text-2xl font-bold">Edit Plan</h3>
                  <p className="text-pink-100 text-sm mt-1">Update plan details</p>
                </div>
                <button
                  onClick={handleCancelEdit}
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
                    Plan Name *
                  </label>
                  <input
                    type="text"
                    value={editPlanForm.name}
                    onChange={(e) => {
                      setEditPlanForm({ ...editPlanForm, name: e.target.value });
                      if (validationErrors.name) {
                        setValidationErrors({...validationErrors, name: ""});
                      }
                    }}
                    placeholder="Plan Name"
                    className={`w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent ${
                      validationErrors.name ? 'border-red-500' : 'border-gray-300'
                    }`}
                  />
                  {validationErrors.name && (
                    <p className="text-red-500 text-xs mt-1 flex items-center gap-1">
                      <AlertCircle className="w-3 h-3" />
                      {validationErrors.name}
                    </p>
                  )}
                </div>

                <div>
                  <label className="block text-sm font-semibold text-gray-700 mb-2">
                    Price *
                  </label>
                  <input
                    type="number"
                    step="0.01"
                    min="0"
                    value={editPlanForm.price}
                    onChange={(e) => {
                      setEditPlanForm({ ...editPlanForm, price: Number(e.target.value) });
                      if (validationErrors.price) {
                        setValidationErrors({...validationErrors, price: ""});
                      }
                    }}
                    placeholder="Price"
                    className={`w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent ${
                      validationErrors.price ? 'border-red-500' : 'border-gray-300'
                    }`}
                  />
                  {validationErrors.price && (
                    <p className="text-red-500 text-xs mt-1 flex items-center gap-1">
                      <AlertCircle className="w-3 h-3" />
                      {validationErrors.price}
                    </p>
                  )}
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
                  <label className="flex items-center gap-2 px-4 py-2 bg-pink-50 border border-pink-200 rounded-lg cursor-pointer hover:bg-pink-100 transition-colors">
                    <input
                      type="checkbox"
                      checked={editPlanForm.popular}
                      onChange={(e) => setEditPlanForm({ ...editPlanForm, popular: e.target.checked })}
                      className="w-5 h-5 text-pink-600 rounded focus:ring-2 focus:ring-pink-500"
                    />
                    <span className="text-sm font-semibold text-gray-700 flex items-center gap-1">
                      <Sparkles className="w-4 h-4 text-pink-600" />
                      Mark as Popular
                    </span>
                  </label>
                </div>
              </div>

              <div>
                <label className="block text-sm font-semibold text-gray-700 mb-2">
                  Description *
                </label>
                <textarea
                  value={editPlanForm.description}
                  onChange={(e) => {
                    setEditPlanForm({ ...editPlanForm, description: e.target.value });
                    if (validationErrors.description) {
                      setValidationErrors({...validationErrors, description: ""});
                    }
                  }}
                  placeholder="Description"
                  rows={3}
                  className={`w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent ${
                    validationErrors.description ? 'border-red-500' : 'border-gray-300'
                  }`}
                />
                {validationErrors.description && (
                  <p className="text-red-500 text-xs mt-1 flex items-center gap-1">
                    <AlertCircle className="w-3 h-3" />
                    {validationErrors.description}
                  </p>
                )}
              </div>

              <div>
                <label className="block text-sm font-semibold text-gray-700 mb-2">
                  Features (comma-separated) *
                </label>
                <textarea
                  value={editPlanForm.features}
                  onChange={(e) => {
                    setEditPlanForm({ ...editPlanForm, features: e.target.value });
                    if (validationErrors.features) {
                      setValidationErrors({...validationErrors, features: ""});
                    }
                  }}
                  placeholder="Features (comma-separated)"
                  rows={4}
                  className={`w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent ${
                    validationErrors.features ? 'border-red-500' : 'border-gray-300'
                  }`}
                />
                {validationErrors.features ? (
                  <p className="text-red-500 text-xs mt-1 flex items-center gap-1">
                    <AlertCircle className="w-3 h-3" />
                    {validationErrors.features}
                  </p>
                ) : (
                  <p className="text-xs text-gray-500 mt-1">Separate each feature with a comma</p>
                )}
              </div>

              {editPlanForm.features && !validationErrors.features && (
                <div className="bg-blue-50 p-3 rounded-lg border border-blue-200">
                  <p className="text-xs font-semibold text-blue-800 mb-2">Preview Features:</p>
                  <ul className="space-y-1">
                    {editPlanForm.features.split(",").map(f => f.trim()).filter(f => f).map((f, i) => (
                      <li key={i} className="flex items-start gap-2 text-xs text-blue-700">
                        <Check className="w-3 h-3 text-blue-500 flex-shrink-0 mt-0.5" />
                        <span>{f}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>

            <div className="flex justify-end gap-3 px-6 pb-6 border-t pt-4">
              <button
                onClick={handleCancelEdit}
                className="px-6 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300 transition-colors font-semibold"
                disabled={isSubmitting}
              >
                Cancel
              </button>
              <button
                onClick={handleSavePlan}
                disabled={isSubmitting}
                className="flex items-center gap-2 px-6 py-2 bg-gradient-to-r from-pink-600 to-purple-600 text-white rounded-lg hover:from-pink-700 hover:to-purple-700 transition-colors font-semibold disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {isSubmitting ? (
                  <>
                    <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white"></div>
                    Saving...
                  </>
                ) : (
                  <>
                    <Save className="w-4 h-4" />
                    Save Changes
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      )}
    </section>
  );
}