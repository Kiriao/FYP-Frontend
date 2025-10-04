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
  Filter
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

  // Helper function to convert Firestore Timestamp to Date
  const getDateFromFirestore = (timestamp: any): Date | null => {
    if (!timestamp) return null;
    if (timestamp.toDate) return timestamp.toDate();
    if (timestamp instanceof Date) return timestamp;
    return new Date(timestamp);
  };

  // Form states
  const [formData, setFormData] = useState({
    code: '',
    discountType: 'percentage' as 'percentage' | 'fixed',
    discountValue: 0,
    isActive: true,
    expiresAt: '',
    usageLimit: 0,
    minPurchase: 0,
    description: ''
  });

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

    return {
      total: promoCodes.length,
      active,
      totalUsed,
      totalDiscount: totalDiscount.toFixed(2)
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

  const resetForm = () => {
    setFormData({
      code: '',
      discountType: 'percentage',
      discountValue: 0,
      isActive: true,
      expiresAt: '',
      usageLimit: 0,
      minPurchase: 0,
      description: ''
    });
    setEditingPromo(null);
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
        description: promo.description || ''
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

  const handleDelete = async (promoId: string, code: string) => {
    if (!confirm(`Are you sure you want to delete promo code "${code}"?`)) return;

    try {
      await deleteDoc(doc(db, 'promoCodes', promoId));
      alert('Promo code deleted successfully!');
      onRefresh();
    } catch (error) {
      console.error('Error deleting promo code:', error);
      alert('Failed to delete promo code.');
    }
  };

  const toggleStatus = async (promo: PromoCode) => {
    try {
      await updateDoc(doc(db, 'promoCodes', promo.id), {
        isActive: !promo.isActive
      });
      onRefresh();
    } catch (error) {
      console.error('Error toggling promo status:', error);
      alert('Failed to update promo code status.');
    }
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
          className="flex items-center gap-2 px-6 py-3 bg-gradient-to-r from-pink-600 to-purple-600 text-white rounded-lg hover:from-pink-700 hover:to-purple-700 transition-all shadow-lg"
        >
          <Plus className="w-5 h-5" />
          Create Promo Code
        </button>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="bg-white rounded-lg shadow p-6 border-l-4 border-purple-500">
          <div className="flex items-center justify-between mb-2">
            <p className="text-sm text-gray-600 font-medium">Total Codes</p>
            <Ticket className="w-5 h-5 text-purple-500" />
          </div>
          <p className="text-3xl font-bold text-gray-800">{metrics.total}</p>
          <p className="text-xs text-gray-500 mt-2">{metrics.active} active</p>
        </div>

        <div className="bg-white rounded-lg shadow p-6 border-l-4 border-green-500">
          <div className="flex items-center justify-between mb-2">
            <p className="text-sm text-gray-600 font-medium">Active Codes</p>
            <CheckCircle className="w-5 h-5 text-green-500" />
          </div>
          <p className="text-3xl font-bold text-gray-800">{metrics.active}</p>
          <p className="text-xs text-gray-500 mt-2">Ready to use</p>
        </div>

        <div className="bg-white rounded-lg shadow p-6 border-l-4 border-blue-500">
          <div className="flex items-center justify-between mb-2">
            <p className="text-sm text-gray-600 font-medium">Total Uses</p>
            <Users className="w-5 h-5 text-blue-500" />
          </div>
          <p className="text-3xl font-bold text-gray-800">{metrics.totalUsed}</p>
          <p className="text-xs text-gray-500 mt-2">Times redeemed</p>
        </div>

        <div className="bg-white rounded-lg shadow p-6 border-l-4 border-orange-500">
          <div className="flex items-center justify-between mb-2">
            <p className="text-sm text-gray-600 font-medium">Total Discount</p>
            <DollarSign className="w-5 h-5 text-orange-500" />
          </div>
          <p className="text-3xl font-bold text-gray-800">${metrics.totalDiscount}</p>
          <p className="text-xs text-gray-500 mt-2">Given to users</p>
        </div>
      </div>

      {/* Filters */}
      <div className="bg-white rounded-lg shadow p-4">
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
        <div className="bg-white rounded-lg shadow p-12 text-center">
          <Ticket className="w-16 h-16 text-gray-300 mx-auto mb-4" />
          <p className="text-gray-500 text-lg">No promo codes found</p>
          <p className="text-gray-400 text-sm mt-2">
            {searchTerm || filterStatus !== 'all' 
              ? 'Try adjusting your filters' 
              : 'Create your first promo code to get started'}
          </p>
        </div>
      ) : (
        <div className="bg-white rounded-lg shadow overflow-hidden">
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
                        <div>
                          <p className="font-bold text-gray-800">{promo.code}</p>
                          {promo.description && (
                            <p className="text-xs text-gray-500 mt-1">{promo.description}</p>
                          )}
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
                                className="bg-blue-600 h-1.5 rounded-full"
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
                            <Calendar className="w-4 h-4 text-gray-400" />
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
                            className={`p-2 rounded-lg transition-colors ${
                              promo.isActive
                                ? 'bg-green-100 text-green-600 hover:bg-green-200'
                                : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
                            }`}
                            title={promo.isActive ? 'Deactivate' : 'Activate'}
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
                            onClick={() => handleDelete(promo.id, promo.code)}
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

      {/* Modal */}
      {showModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-xl shadow-2xl max-w-2xl w-full max-h-[90vh] overflow-y-auto">
            <div className="sticky top-0 bg-gradient-to-r from-pink-600 to-purple-600 text-white px-6 py-4 rounded-t-xl">
              <h3 className="text-2xl font-bold">
                {editingPromo ? 'Edit Promo Code' : 'Create New Promo Code'}
              </h3>
            </div>

            <form onSubmit={handleSubmit} className="p-6 space-y-5">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-semibold text-gray-700 mb-2">
                    Promo Code *
                  </label>
                  <input
                    type="text"
                    required
                    value={formData.code}
                    onChange={(e) => setFormData({ ...formData, code: e.target.value.toUpperCase() })}
                    placeholder="SAVE20"
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent uppercase"
                    disabled={!!editingPromo}
                  />
                </div>

                <div>
                  <label className="block text-sm font-semibold text-gray-700 mb-2">
                    Discount Type *
                  </label>
                  <select
                    value={formData.discountType}
                    onChange={(e) => setFormData({ ...formData, discountType: e.target.value as any })}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
                  >
                    <option value="percentage">Percentage</option>
                    <option value="fixed">Fixed Amount</option>
                  </select>
                </div>

                <div>
                  <label className="block text-sm font-semibold text-gray-700 mb-2">
                    Discount Value *
                  </label>
                  <input
                    type="number"
                    required
                    min="0"
                    step="0.01"
                    value={formData.discountValue}
                    onChange={(e) => setFormData({ ...formData, discountValue: parseFloat(e.target.value) })}
                    placeholder={formData.discountType === 'percentage' ? '20' : '10.00'}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
                  />
                </div>

                <div>
                  <label className="block text-sm font-semibold text-gray-700 mb-2">
                    Expiry Date
                  </label>
                  <input
                    type="date"
                    value={formData.expiresAt}
                    onChange={(e) => setFormData({ ...formData, expiresAt: e.target.value })}
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
                  />
                </div>

                <div>
                  <label className="block text-sm font-semibold text-gray-700 mb-2">
                    Usage Limit
                  </label>
                  <input
                    type="number"
                    min="0"
                    value={formData.usageLimit}
                    onChange={(e) => setFormData({ ...formData, usageLimit: parseInt(e.target.value) })}
                    placeholder="0 = unlimited"
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
                  />
                </div>

                <div>
                  <label className="block text-sm font-semibold text-gray-700 mb-2">
                    Minimum Purchase ($)
                  </label>
                  <input
                    type="number"
                    min="0"
                    step="0.01"
                    value={formData.minPurchase}
                    onChange={(e) => setFormData({ ...formData, minPurchase: parseFloat(e.target.value) })}
                    placeholder="0 = no minimum"
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
                  />
                </div>
              </div>

              <div>
                <label className="block text-sm font-semibold text-gray-700 mb-2">
                  Description
                </label>
                <textarea
                  value={formData.description}
                  onChange={(e) => setFormData({ ...formData, description: e.target.value })}
                  placeholder="Internal note about this promo code"
                  rows={3}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-500 focus:border-transparent"
                />
              </div>

              <div className="flex items-center gap-3">
                <input
                  type="checkbox"
                  id="isActive"
                  checked={formData.isActive}
                  onChange={(e) => setFormData({ ...formData, isActive: e.target.checked })}
                  className="w-5 h-5 text-pink-600 border-gray-300 rounded focus:ring-pink-500"
                />
                <label htmlFor="isActive" className="text-sm font-medium text-gray-700">
                  Active (users can use this code)
                </label>
              </div>

              <div className="flex gap-3 pt-4">
                <button
                  type="submit"
                  className="flex-1 px-6 py-3 bg-gradient-to-r from-pink-600 to-purple-600 text-white font-semibold rounded-lg hover:from-pink-700 hover:to-purple-700 transition-all"
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