'use client';

import { useState, useMemo } from 'react';
import { db } from '@/lib/firebase';
import { collection, addDoc, updateDoc, deleteDoc, doc, serverTimestamp, Timestamp } from 'firebase/firestore';
import {
  Ticket,
  Plus,
  Edit2,
  Trash2,
  CheckCircle,
  XCircle,
  Calendar,
  Users,
  DollarSign,
  Percent,
  Search,
  Filter,
  AlertCircle,
  Copy,
  Check,
  TrendingUp,
  Clock
} from 'lucide-react';

interface PromoCode {
  id: string;
  code: string;
  discountType: 'percentage' | 'fixed';
  discountValue: number;
  isActive: boolean;
  expiresAt?: Timestamp | null;
  usageLimit?: number;
  usedCount?: number;
  minPurchase?: number;
  createdAt?: Timestamp;
  description?: string;
  maxDiscount?: number;
  firstTimeOnly?: boolean;
}

interface PromoCodesTabProps {
  promoCodes: PromoCode[];
  onRefresh: () => void;
}

export default function PromoCodesTab({ promoCodes, onRefresh }: PromoCodesTabProps) {
  const [showModal, setShowModal] = useState(false);
  const [editingPromo, setEditingPromo] = useState<PromoCode | null>(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [filterStatus, setFilterStatus] = useState<'all' | 'active' | 'expired' | 'exhausted'>('all');
  const [copiedCode, setCopiedCode] = useState<string | null>(null);
  const [validationErrors, setValidationErrors] = useState<Record<string, string>>({});

  const [formData, setFormData] = useState({
    code: '',
    discountType: 'percentage' as 'percentage' | 'fixed',
    discountValue: 0,
    isActive: true,
    expiresAt: '',
    usageLimit: 0,
    minPurchase: 0,
    description: '',
    maxDiscount: 0,
    firstTimeOnly: false
  });

  // Helper function to convert Firestore Timestamp to Date
  const getDateFromFirestore = (timestamp: any): Date | null => {
    if (!timestamp) return null;
    if (timestamp.toDate) return timestamp.toDate();
    if (timestamp instanceof Date) return timestamp;
    return new Date(timestamp);
  };

  // Calculate metrics
  const metrics = useMemo(() => {
    const active = promoCodes.filter(p => {
      if (!p.isActive) return false;
      
      if (p.expiresAt) {
        const expiryDate = getDateFromFirestore(p.expiresAt);
        if (expiryDate && expiryDate <= new Date()) return false;
      }
      
      if (p.usageLimit && p.usedCount && p.usedCount >= p.usageLimit) return false;
      
      return true;
    }).length;

    const totalUsed = promoCodes.reduce((sum, p) => sum + (p.usedCount || 0), 0);
    
    const totalDiscount = promoCodes.reduce((sum, p) => {
      const uses = p.usedCount || 0;
      if (p.discountType === 'percentage') {
        return sum + (uses * 10); // Estimate average discount
      }
      return sum + (uses * p.discountValue);
    }, 0);

    const avgUsage = promoCodes.length > 0 
      ? (totalUsed / promoCodes.length).toFixed(1) 
      : '0';

    return {
      total: promoCodes.length,
      active,
      totalUsed,
      totalDiscount: totalDiscount.toFixed(2),
      avgUsage
    };
  }, [promoCodes]);

  // Filter promo codes
  const filteredPromoCodes = useMemo(() => {
    return promoCodes.filter(promo => {
      const matchesSearch = promo.code.toLowerCase().includes(searchTerm.toLowerCase()) ||
        promo.description?.toLowerCase().includes(searchTerm.toLowerCase());

      let matchesStatus = true;
      if (filterStatus === 'active') {
        if (!promo.isActive) return false;
        
        if (promo.expiresAt) {
          const expiryDate = getDateFromFirestore(promo.expiresAt);
          if (expiryDate && expiryDate <= new Date()) return false;
        }
        
        if (promo.usageLimit && promo.usedCount && promo.usedCount >= promo.usageLimit) return false;
        
      } else if (filterStatus === 'expired') {
        if (!promo.expiresAt) return false;
        const expiryDate = getDateFromFirestore(promo.expiresAt);
        matchesStatus = expiryDate ? expiryDate <= new Date() : false;
        
      } else if (filterStatus === 'exhausted') {
        matchesStatus = promo.usageLimit ? (promo.usedCount || 0) >= promo.usageLimit : false;
      }

      return matchesSearch && matchesStatus;
    });
  }, [promoCodes, searchTerm, filterStatus]);

  const validateForm = () => {
    const errors: Record<string, string> = {};

    if (!formData.code.trim()) {
      errors.code = 'Promo code is required';
    } else if (formData.code.length < 3) {
      errors.code = 'Code must be at least 3 characters';
    } else if (!/^[A-Z0-9]+$/.test(formData.code)) {
      errors.code = 'Code must contain only letters and numbers';
    } else if (!editingPromo && promoCodes.some(p => p.code === formData.code)) {
      errors.code = 'This code already exists';
    }

    if (formData.discountValue <= 0) {
      errors.discountValue = 'Discount value must be greater than 0';
    }

    if (formData.discountType === 'percentage') {
      if (formData.discountValue > 100) {
        errors.discountValue = 'Percentage cannot exceed 100%';
      }
    }

    if (formData.expiresAt) {
      const expiryDate = new Date(formData.expiresAt);
      if (expiryDate <= new Date()) {
        errors.expiresAt = 'Expiry date must be in the future';
      }
    }

    if (formData.usageLimit < 0) {
      errors.usageLimit = 'Usage limit cannot be negative';
    }

    if (formData.minPurchase < 0) {
      errors.minPurchase = 'Minimum purchase cannot be negative';
    }

    if (formData.discountType === 'percentage' && formData.maxDiscount > 0) {
      if (formData.maxDiscount < 1) {
        errors.maxDiscount = 'Maximum discount must be at least $1';
      }
    }

    setValidationErrors(errors);
    return Object.keys(errors).length === 0;
  };

  const generateRandomCode = () => {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    let code = '';
    for (let i = 0; i < 8; i++) {
      code += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    setFormData({ ...formData, code });
    setValidationErrors({ ...validationErrors, code: '' });
  };

  const resetForm = () => {
    setFormData({
      code: '',
      discountType: 'percentage',
      discountValue: 0,
      isActive: true,
      expiresAt: '',
      usageLimit: 0,
      minPurchase: 0,
      description: '',
      maxDiscount: 0,
      firstTimeOnly: false
    });
    setEditingPromo(null);
    setValidationErrors({});
  };

  const handleOpenModal = (promo?: PromoCode) => {
    if (promo) {
      setEditingPromo(promo);
      
      const expiryDate = getDateFromFirestore(promo.expiresAt);
      const expiresAtString = expiryDate ? expiryDate.toISOString().split('T')[0] : '';
      
      setFormData({
        code: promo.code,
        discountType: promo.discountType,
        discountValue: promo.discountValue,
        isActive: promo.isActive,
        expiresAt: expiresAtString,
        usageLimit: promo.usageLimit || 0,
        minPurchase: promo.minPurchase || 0,
        description: promo.description || '',
        maxDiscount: promo.maxDiscount || 0,
        firstTimeOnly: promo.firstTimeOnly || false
      });
    } else {
      resetForm();
    }
    setShowModal(true);
  };

  const handleCloseModal = () => {
    setShowModal(false);
    resetForm();
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    if (!validateForm()) {
      return;
    }

    try {
      const promoData = {
        code: formData.code.toUpperCase(),
        discountType: formData.discountType,
        discountValue: Number(formData.discountValue),
        isActive: formData.isActive,
        expiresAt: formData.expiresAt ? Timestamp.fromDate(new Date(formData.expiresAt)) : null,
        usageLimit: formData.usageLimit > 0 ? Number(formData.usageLimit) : null,
        minPurchase: formData.minPurchase > 0 ? Number(formData.minPurchase) : null,
        description: formData.description || null,
        maxDiscount: formData.maxDiscount > 0 ? Number(formData.maxDiscount) : null,
        firstTimeOnly: formData.firstTimeOnly,
        usedCount: editingPromo?.usedCount || 0,
        ...(editingPromo ? {} : { createdAt: serverTimestamp() })
      };

      if (editingPromo) {
        await updateDoc(doc(db, 'promoCodes', editingPromo.id), promoData);
        alert('Promo code updated successfully!');
      } else {
        await addDoc(collection(db, 'promoCodes'), promoData);
        alert('Promo code created successfully!');
      }

      handleCloseModal();
      onRefresh();
    } catch (error) {
      console.error('Error saving promo code:', error);
      alert('Failed to save promo code. Please try again.');
    }
  };

  const handleDelete = async (promoId: string, code: string, promo: PromoCode) => {
    // Check if promo code has been used
    const hasBeenUsed = (promo.usedCount || 0) > 0;
    
    // Build confirmation message based on usage
    let confirmMessage = `Are you sure you want to delete the promo code "${code}"?\n\n`;
    
    if (hasBeenUsed) {
      confirmMessage += `⚠️ WARNING: This code has been used ${promo.usedCount} time${promo.usedCount === 1 ? '' : 's'}.\n`;
      confirmMessage += `Deleting it may affect reporting and historical data.\n\n`;
      confirmMessage += `Consider deactivating instead of deleting.\n\n`;
    }
    
    confirmMessage += `This action cannot be undone.`;

    if (!confirm(confirmMessage)) return;

    // Extra confirmation for codes that have been used
    if (hasBeenUsed) {
      const extraConfirm = confirm(
        `Final confirmation: Type the code name to confirm deletion.\n\nDo you really want to delete "${code}"?`
      );
      if (!extraConfirm) return;
    }

    try {
      await deleteDoc(doc(db, 'promoCodes', promoId));
      alert(`Promo code "${code}" has been deleted successfully!`);
      onRefresh();
    } catch (error) {
      console.error('Error deleting promo code:', error);
      alert('Failed to delete promo code. Please try again.');
    }
  };

  const toggleStatus = async (promo: PromoCode) => {
    // Check if promo code is expired
    const expiryDate = getDateFromFirestore(promo.expiresAt);
    if (expiryDate && expiryDate <= new Date()) {
      alert('Cannot change status of an expired promo code.');
      return;
    }

    // Confirmation message
    const action = promo.isActive ? 'deactivate' : 'activate';
    const confirmMessage = promo.isActive 
      ? `Are you sure you want to deactivate "${promo.code}"? Users will no longer be able to use this code.`
      : `Are you sure you want to activate "${promo.code}"? Users will be able to use this code immediately.`;

    if (!confirm(confirmMessage)) return;

    try {
      await updateDoc(doc(db, 'promoCodes', promo.id), {
        isActive: !promo.isActive
      });
      alert(`Promo code "${promo.code}" has been ${action}d successfully!`);
      onRefresh();
    } catch (error) {
      console.error('Error toggling promo status:', error);
      alert('Failed to update promo code status.');
    }
  };

  const copyCode = (code: string) => {
    navigator.clipboard.writeText(code);
    setCopiedCode(code);
    setTimeout(() => setCopiedCode(null), 2000);
  };

  const getPromoStatus = (promo: PromoCode) => {
    if (!promo.isActive) return { label: 'Inactive', color: 'bg-gray-100 text-gray-700' };
    
    const expiryDate = getDateFromFirestore(promo.expiresAt);
    if (expiryDate && expiryDate <= new Date()) {
      return { label: 'Expired', color: 'bg-red-100 text-red-700' };
    }
    
    if (promo.usageLimit && promo.usedCount && promo.usedCount >= promo.usageLimit) {
      return { label: 'Exhausted', color: 'bg-orange-100 text-orange-700' };
    }
    return { label: 'Active', color: 'bg-green-100 text-green-700' };
  };

  return (
    <section className="space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h2 className="text-3xl font-bold text-gray-800">Promo Code Management</h2>
          <p className="text-gray-600 mt-1">Create and manage discount codes</p>
        </div>
        <button
          onClick={() => handleOpenModal()}
          className="flex items-center gap-2 px-6 py-3 bg-gradient-to-r from-pink-600 to-purple-600 text-white rounded-lg hover:from-pink-700 hover:to-purple-700 transition-all shadow-lg hover:shadow-xl transform hover:-translate-y-0.5"
        >
          <Plus className="w-5 h-5" />
          Create Promo Code
        </button>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="bg-white rounded-xl shadow-md p-6 border-l-4 border-purple-500 hover:shadow-lg transition-shadow">
          <div className="flex items-center justify-between mb-2">
            <p className="text-sm text-gray-600 font-medium">Total Codes</p>
            <Ticket className="w-5 h-5 text-purple-500" />
          </div>
          <p className="text-3xl font-bold text-gray-800">{metrics.total}</p>
          <p className="text-xs text-gray-500 mt-2">{metrics.active} active</p>
        </div>

        <div className="bg-white rounded-xl shadow-md p-6 border-l-4 border-green-500 hover:shadow-lg transition-shadow">
          <div className="flex items-center justify-between mb-2">
            <p className="text-sm text-gray-600 font-medium">Active Codes</p>
            <CheckCircle className="w-5 h-5 text-green-500" />
          </div>
          <p className="text-3xl font-bold text-gray-800">{metrics.active}</p>
          <p className="text-xs text-gray-500 mt-2">Ready to use</p>
        </div>

        <div className="bg-white rounded-xl shadow-md p-6 border-l-4 border-blue-500 hover:shadow-lg transition-shadow">
          <div className="flex items-center justify-between mb-2">
            <p className="text-sm text-gray-600 font-medium">Total Uses</p>
            <TrendingUp className="w-5 h-5 text-blue-500" />
          </div>
          <p className="text-3xl font-bold text-gray-800">{metrics.totalUsed}</p>
          <p className="text-xs text-gray-500 mt-2">Avg: {metrics.avgUsage} per code</p>
        </div>

        <div className="bg-white rounded-xl shadow-md p-6 border-l-4 border-orange-500 hover:shadow-lg transition-shadow">
          <div className="flex items-center justify-between mb-2">
            <p className="text-sm text-gray-600 font-medium">Total Discount</p>
            <DollarSign className="w-5 h-5 text-orange-500" />
          </div>
          <p className="text-3xl font-bold text-gray-800">${metrics.totalDiscount}</p>
          <p className="text-xs text-gray-500 mt-2">Given to users</p>
        </div>
      </div>

      {/* Filters */}
      <div className="bg-white rounded-xl shadow-md p-4">
        <div className="flex flex-col md:flex-row gap-4">
          <div className="flex-1 relative">
            <Search className="w-5 h-5 text-gray-400 absolute left-3 top-1/2 transform -translate-y-1/2" />
            <input
              type="text"
              placeholder="Search promo codes..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
            />
          </div>
          <div className="flex items-center gap-2">
            <Filter className="w-5 h-5 text-gray-400" />
            <select
              value={filterStatus}
              onChange={(e) => setFilterStatus(e.target.value as any)}
              className="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
            >
              <option value="all">All Codes</option>
              <option value="active">Active Only</option>
              <option value="expired">Expired</option>
              <option value="exhausted">Exhausted</option>
            </select>
          </div>
        </div>
      </div>

      {/* Promo Codes Table */}
      {filteredPromoCodes.length === 0 ? (
        <div className="bg-white rounded-xl shadow-md p-12 text-center">
          <Ticket className="w-16 h-16 text-gray-300 mx-auto mb-4" />
          <p className="text-gray-500 text-lg">No promo codes found</p>
          <p className="text-gray-400 text-sm mt-2">
            {searchTerm || filterStatus !== 'all' 
              ? 'Try adjusting your filters' 
              : 'Create your first promo code to get started'}
          </p>
        </div>
      ) : (
        <div className="bg-white rounded-xl shadow-md overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-gradient-to-r from-pink-50 to-purple-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                    Code
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                    Discount
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                    Usage
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                    Expiry
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                    Status
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {filteredPromoCodes.map((promo) => {
                  const status = getPromoStatus(promo);
                  return (
                    <tr key={promo.id} className="hover:bg-gray-50 transition-colors">
                      <td className="px-6 py-4">
                        <div className="flex items-start gap-2">
                          <div className="flex-1">
                            <div className="flex items-center gap-2">
                              <p className="font-bold text-gray-800">{promo.code}</p>
                              <button
                                onClick={() => copyCode(promo.code)}
                                className="text-gray-400 hover:text-gray-600 transition-colors"
                                title="Copy code"
                              >
                                {copiedCode === promo.code ? (
                                  <Check className="w-4 h-4 text-green-500" />
                                ) : (
                                  <Copy className="w-4 h-4" />
                                )}
                              </button>
                            </div>
                            {promo.description && (
                              <p className="text-xs text-gray-500 mt-1">{promo.description}</p>
                            )}
                            {promo.firstTimeOnly && (
                              <span className="inline-block mt-1 px-2 py-0.5 bg-blue-100 text-blue-700 text-xs rounded-full">
                                First-time only
                              </span>
                            )}
                          </div>
                        </div>
                      </td>
                      <td className="px-6 py-4">
                        <div className="flex items-center gap-2">
                          {promo.discountType === 'percentage' ? (
                            <>
                              <Percent className="w-4 h-4 text-purple-600" />
                              <span className="font-semibold text-purple-600">
                                {promo.discountValue}% off
                              </span>
                            </>
                          ) : (
                            <>
                              <DollarSign className="w-4 h-4 text-green-600" />
                              <span className="font-semibold text-green-600">
                                ${promo.discountValue} off
                              </span>
                            </>
                          )}
                        </div>
                        {promo.minPurchase && (
                          <p className="text-xs text-gray-500 mt-1">
                            Min: ${promo.minPurchase}
                          </p>
                        )}
                        {promo.maxDiscount && promo.discountType === 'percentage' && (
                          <p className="text-xs text-gray-500 mt-1">
                            Max: ${promo.maxDiscount}
                          </p>
                        )}
                      </td>
                      <td className="px-6 py-4">
                        <div>
                          <p className="text-sm font-medium text-gray-800">
                            {promo.usedCount || 0}
                            {promo.usageLimit && ` / ${promo.usageLimit}`}
                          </p>
                          {promo.usageLimit && (
                            <div className="w-24 bg-gray-200 rounded-full h-1.5 mt-1">
                              <div
                                className="bg-blue-600 h-1.5 rounded-full transition-all"
                                style={{
                                  width: `${Math.min(((promo.usedCount || 0) / promo.usageLimit) * 100, 100)}%`
                                }}
                              />
                            </div>
                          )}
                        </div>
                      </td>
                      <td className="px-6 py-4">
                        {promo.expiresAt ? (
                          <div className="flex items-center gap-1 text-sm">
                            <Clock className="w-4 h-4 text-gray-400" />
                            <span className="text-gray-600">
                              {getDateFromFirestore(promo.expiresAt)?.toLocaleDateString() || 'Invalid date'}
                            </span>
                          </div>
                        ) : (
                          <span className="text-gray-400 text-sm">No expiry</span>
                        )}
                      </td>
                      <td className="px-6 py-4">
                        <span className={`px-3 py-1 rounded-full text-xs font-semibold ${status.color}`}>
                          {status.label}
                        </span>
                      </td>
                      <td className="px-6 py-4">
                        <div className="flex items-center gap-2">
                          <button
                            onClick={() => toggleStatus(promo)}
                            disabled={!!(promo.expiresAt && getDateFromFirestore(promo.expiresAt) && getDateFromFirestore(promo.expiresAt)! <= new Date())}
                            className={`p-2 rounded-lg transition-colors ${
                              promo.expiresAt && getDateFromFirestore(promo.expiresAt) && getDateFromFirestore(promo.expiresAt)! <= new Date()
                                ? 'bg-gray-100 text-gray-400 cursor-not-allowed opacity-50'
                                : promo.isActive
                                ? 'bg-green-100 text-green-600 hover:bg-green-200'
                                : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                            }`}
                            title={
                              promo.expiresAt && getDateFromFirestore(promo.expiresAt) && getDateFromFirestore(promo.expiresAt)! <= new Date()
                                ? 'Expired codes cannot be activated/deactivated'
                                : promo.isActive 
                                ? 'Deactivate' 
                                : 'Activate'
                            }
                          >
                            {promo.isActive ? (
                              <CheckCircle className="w-4 h-4" />
                            ) : (
                              <XCircle className="w-4 h-4" />
                            )}
                          </button>
                          <button
                            onClick={() => handleOpenModal(promo)}
                            className="p-2 bg-blue-100 text-blue-600 rounded-lg hover:bg-blue-200 transition-colors"
                            title="Edit"
                          >
                            <Edit2 className="w-4 h-4" />
                          </button>
                          <button
                            onClick={() => handleDelete(promo.id, promo.code, promo)}
                            className="p-2 bg-red-100 text-red-600 rounded-lg hover:bg-red-200 transition-colors"
                            title="Delete"
                          >
                            <Trash2 className="w-4 h-4" />
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Enhanced Modal */}
      {showModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-2xl shadow-2xl max-w-3xl w-full max-h-[90vh] overflow-y-auto">
            <div className="sticky top-0 bg-gradient-to-r from-pink-600 to-purple-600 text-white px-6 py-4 rounded-t-2xl">
              <h3 className="text-2xl font-bold">
                {editingPromo ? 'Edit Promo Code' : 'Create New Promo Code'}
              </h3>
              <p className="text-pink-100 text-sm mt-1">
                {editingPromo ? 'Update your promotional offer' : 'Set up a new discount for your customers'}
              </p>
            </div>

            <form onSubmit={handleSubmit} className="p-6 space-y-6">
              {/* Code Generation Section */}
              <div className="bg-gradient-to-br from-purple-50 to-pink-50 rounded-lg p-4 border border-purple-200">
                <label className="block text-sm font-semibold text-gray-700 mb-2">
                  Promo Code *
                </label>
                <div className="flex gap-2">
                  <div className="flex-1">
                    <input
                      type="text"
                      required
                      value={formData.code}
                      onChange={(e) => {
                        setFormData({ ...formData, code: e.target.value.toUpperCase() });
                        setValidationErrors({ ...validationErrors, code: '' });
                      }}
                      placeholder="SAVE20"
                      className={`w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent uppercase font-mono text-lg ${
                        validationErrors.code ? 'border-red-500' : 'border-gray-300'
                      }`}
                      disabled={!!editingPromo}
                      maxLength={20}
                    />
                    {validationErrors.code && (
                      <p className="text-red-500 text-xs mt-1 flex items-center gap-1">
                        <AlertCircle className="w-3 h-3" />
                        {validationErrors.code}
                      </p>
                    )}
                  </div>
                  {!editingPromo && (
                    <button
                      type="button"
                      onClick={generateRandomCode}
                      className="px-4 py-2 bg-white border border-purple-300 text-purple-600 rounded-lg hover:bg-purple-50 transition-colors font-semibold"
                    >
                      Generate
                    </button>
                  )}
                </div>
                <p className="text-xs text-gray-500 mt-2">
                  Use uppercase letters and numbers only. 3-20 characters.
                </p>
              </div>

              {/* Discount Configuration */}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-semibold text-gray-700 mb-2">
                    Discount Type *
                  </label>
                  <select
                    value={formData.discountType}
                    onChange={(e) => {
                      setFormData({ ...formData, discountType: e.target.value as any });
                      setValidationErrors({ ...validationErrors, discountValue: '' });
                    }}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
                  >
                    <option value="percentage">Percentage Discount</option>
                    <option value="fixed">Fixed Amount</option>
                  </select>
                </div>

                <div>
                  <label className="block text-sm font-semibold text-gray-700 mb-2">
                    Discount Value *
                  </label>
                  <div className="relative">
                    {formData.discountType === 'percentage' && (
                      <Percent className="w-5 h-5 text-gray-400 absolute right-3 top-1/2 transform -translate-y-1/2" />
                    )}
                    {formData.discountType === 'fixed' && (
                      <DollarSign className="w-5 h-5 text-gray-400 absolute left-3 top-1/2 transform -translate-y-1/2" />
                    )}
                    <input
                      type="number"
                      required
                      min="0"
                      step="0.01"
                      value={formData.discountValue || ''}
                      onChange={(e) => {
                        setFormData({ ...formData, discountValue: parseFloat(e.target.value) || 0 });
                        setValidationErrors({ ...validationErrors, discountValue: '' });
                      }}
                      placeholder={formData.discountType === 'percentage' ? '20' : '10.00'}
                      className={`w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent ${
                        formData.discountType === 'fixed' ? 'pl-10' : ''
                      } ${validationErrors.discountValue ? 'border-red-500' : 'border-gray-300'}`}
                    />
                  </div>
                  {validationErrors.discountValue && (
                    <p className="text-red-500 text-xs mt-1 flex items-center gap-1">
                      <AlertCircle className="w-3 h-3" />
                      {validationErrors.discountValue}
                    </p>
                  )}
                  <p className="text-xs text-gray-500 mt-1">
                    {formData.discountType === 'percentage' 
                      ? 'Enter percentage value (1-100)' 
                      : 'Enter dollar amount'}
                  </p>
                </div>

                {formData.discountType === 'percentage' && (
                  <div>
                    <label className="block text-sm font-semibold text-gray-700 mb-2">
                      Maximum Discount Cap ($)
                      <span className="text-gray-500 font-normal ml-1">(Optional)</span>
                    </label>
                    <input
                      type="number"
                      min="0"
                      step="0.01"
                      value={formData.maxDiscount || ''}
                      onChange={(e) => {
                        setFormData({ ...formData, maxDiscount: parseFloat(e.target.value) || 0 });
                        setValidationErrors({ ...validationErrors, maxDiscount: '' });
                      }}
                      placeholder="e.g., 50"
                      className={`w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent ${
                        validationErrors.maxDiscount ? 'border-red-500' : 'border-gray-300'
                      }`}
                    />
                    {validationErrors.maxDiscount && (
                      <p className="text-red-500 text-xs mt-1 flex items-center gap-1">
                        <AlertCircle className="w-3 h-3" />
                        {validationErrors.maxDiscount}
                      </p>
                    )}
                    <p className="text-xs text-gray-500 mt-1">
                      Limit the maximum discount amount (e.g., 20% off with $50 max)
                    </p>
                  </div>
                )}

                <div>
                  <label className="block text-sm font-semibold text-gray-700 mb-2">
                    Minimum Purchase ($)
                    <span className="text-gray-500 font-normal ml-1">(Optional)</span>
                  </label>
                  <input
                    type="number"
                    min="0"
                    step="0.01"
                    value={formData.minPurchase || ''}
                    onChange={(e) => {
                      setFormData({ ...formData, minPurchase: parseFloat(e.target.value) || 0 });
                      setValidationErrors({ ...validationErrors, minPurchase: '' });
                    }}
                    placeholder="0 = no minimum"
                    className={`w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent ${
                      validationErrors.minPurchase ? 'border-red-500' : 'border-gray-300'
                    }`}
                  />
                  {validationErrors.minPurchase && (
                    <p className="text-red-500 text-xs mt-1 flex items-center gap-1">
                      <AlertCircle className="w-3 h-3" />
                      {validationErrors.minPurchase}
                    </p>
                  )}
                </div>
              </div>

              {/* Usage Limits */}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-semibold text-gray-700 mb-2">
                    <div className="flex items-center gap-2">
                      <Users className="w-4 h-4" />
                      Usage Limit
                      <span className="text-gray-500 font-normal">(Optional)</span>
                    </div>
                  </label>
                  <input
                    type="number"
                    min="0"
                    value={formData.usageLimit || ''}
                    onChange={(e) => {
                      setFormData({ ...formData, usageLimit: parseInt(e.target.value) || 0 });
                      setValidationErrors({ ...validationErrors, usageLimit: '' });
                    }}
                    placeholder="0 = unlimited"
                    className={`w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent ${
                      validationErrors.usageLimit ? 'border-red-500' : 'border-gray-300'
                    }`}
                  />
                  {validationErrors.usageLimit && (
                    <p className="text-red-500 text-xs mt-1 flex items-center gap-1">
                      <AlertCircle className="w-3 h-3" />
                      {validationErrors.usageLimit}
                    </p>
                  )}
                  <p className="text-xs text-gray-500 mt-1">
                    Total number of times this code can be used
                  </p>
                </div>

                <div>
                  <label className="block text-sm font-semibold text-gray-700 mb-2">
                    <div className="flex items-center gap-2">
                      <Calendar className="w-4 h-4" />
                      Expiry Date
                      <span className="text-gray-500 font-normal">(Optional)</span>
                    </div>
                  </label>
                  <input
                    type="date"
                    value={formData.expiresAt}
                    onChange={(e) => {
                      setFormData({ ...formData, expiresAt: e.target.value });
                      setValidationErrors({ ...validationErrors, expiresAt: '' });
                    }}
                    min={new Date().toISOString().split('T')[0]}
                    className={`w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent ${
                      validationErrors.expiresAt ? 'border-red-500' : 'border-gray-300'
                    }`}
                  />
                  {validationErrors.expiresAt && (
                    <p className="text-red-500 text-xs mt-1 flex items-center gap-1">
                      <AlertCircle className="w-3 h-3" />
                      {validationErrors.expiresAt}
                    </p>
                  )}
                  <p className="text-xs text-gray-500 mt-1">
                    Leave empty for no expiration
                  </p>
                </div>
              </div>

              {/* Description */}
              <div>
                <label className="block text-sm font-semibold text-gray-700 mb-2">
                  Description / Notes
                  <span className="text-gray-500 font-normal ml-1">(Optional)</span>
                </label>
                <textarea
                  value={formData.description}
                  onChange={(e) => setFormData({ ...formData, description: e.target.value })}
                  placeholder="Internal note about this promo code (e.g., 'Holiday Sale 2024')"
                  rows={3}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
                  maxLength={200}
                />
                <p className="text-xs text-gray-500 mt-1">
                  {formData.description.length}/200 characters
                </p>
              </div>

              {/* Advanced Options */}
              <div className="bg-gray-50 rounded-lg p-4 space-y-3">
                <h4 className="font-semibold text-gray-700 text-sm">Advanced Options</h4>
                
                <div className="flex items-center gap-3">
                  <input
                    type="checkbox"
                    id="isActive"
                    checked={formData.isActive}
                    onChange={(e) => setFormData({ ...formData, isActive: e.target.checked })}
                    className="w-5 h-5 text-pink-600 border-gray-300 rounded focus:ring-pink-500"
                  />
                  <label htmlFor="isActive" className="text-sm font-medium text-gray-700 cursor-pointer">
                    <div className="flex items-center gap-2">
                      <CheckCircle className="w-4 h-4 text-green-600" />
                      Active (users can immediately use this code)
                    </div>
                  </label>
                </div>

                <div className="flex items-center gap-3">
                  <input
                    type="checkbox"
                    id="firstTimeOnly"
                    checked={formData.firstTimeOnly}
                    onChange={(e) => setFormData({ ...formData, firstTimeOnly: e.target.checked })}
                    className="w-5 h-5 text-pink-600 border-gray-300 rounded focus:ring-pink-500"
                  />
                  <label htmlFor="firstTimeOnly" className="text-sm font-medium text-gray-700 cursor-pointer">
                    <div className="flex items-center gap-2">
                      <Users className="w-4 h-4 text-blue-600" />
                      First-time customers only
                    </div>
                  </label>
                </div>
              </div>

              {/* Preview Section */}
              {formData.code && formData.discountValue > 0 && (
                <div className="bg-gradient-to-r from-pink-50 to-purple-50 rounded-lg p-4 border-2 border-dashed border-purple-300">
                  <p className="text-sm font-semibold text-gray-700 mb-2">Preview:</p>
                  <div className="bg-white rounded-lg p-3 border border-purple-200">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="font-bold text-lg text-purple-600">{formData.code}</p>
                        <p className="text-sm text-gray-600">
                          Get {formData.discountType === 'percentage' 
                            ? `${formData.discountValue}% off` 
                            : `${formData.discountValue} off`}
                          {formData.minPurchase > 0 && ` on orders over ${formData.minPurchase}`}
                        </p>
                      </div>
                      <Ticket className="w-8 h-8 text-purple-400" />
                    </div>
                  </div>
                </div>
              )}

              {/* Action Buttons */}
              <div className="flex gap-3 pt-4">
                <button
                  type="submit"
                  className="flex-1 px-6 py-3 bg-gradient-to-r from-pink-600 to-purple-600 text-white font-semibold rounded-lg hover:from-pink-700 hover:to-purple-700 transition-all shadow-lg hover:shadow-xl transform hover:-translate-y-0.5"
                >
                  {editingPromo ? 'Update Promo Code' : 'Create Promo Code'}
                </button>
                <button
                  type="button"
                  onClick={handleCloseModal}
                  className="px-6 py-3 bg-gray-200 text-gray-700 font-semibold rounded-lg hover:bg-gray-300 transition-colors"
                >
                  Cancel
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </section>
  );
}