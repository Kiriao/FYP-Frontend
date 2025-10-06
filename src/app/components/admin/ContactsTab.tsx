'use client';

import { useState } from 'react';
import { doc, updateDoc, deleteDoc } from 'firebase/firestore';
import { db } from '@/lib/firebase';
import { Mail, Calendar, User, MessageSquare, Tag, Check, X, Trash2, Filter, Search } from 'lucide-react';

interface ContactData {
  uid: string;
  name: string;
  email: string;
  subject: string;
  message: string;
  createdAt: any;
  status?: string;
}

interface ContactsTabProps {
  contacts: ContactData[];
}

export default function ContactsTab({ contacts }: ContactsTabProps) {
  const [filter, setFilter] = useState<string>('all');
  const [searchTerm, setSearchTerm] = useState('');
  const [expandedContact, setExpandedContact] = useState<string | null>(null);
  const [processing, setProcessing] = useState<string | null>(null);

  const subjectLabels: Record<string, string> = {
    general: "General Inquiry",
    support: "Technical Support",
    feedback: "Feedback & Suggestions",
    partnership: "Partnership Opportunities",
    content: "Content Requests",
    billing: "Billing Questions",
    other: "Other"
  };

  const statusColors: Record<string, { bg: string; text: string; border: string }> = {
    new: { bg: 'bg-blue-50', text: 'text-blue-700', border: 'border-blue-200' },
    pending: { bg: 'bg-yellow-50', text: 'text-yellow-700', border: 'border-yellow-200' },
    resolved: { bg: 'bg-green-50', text: 'text-green-700', border: 'border-green-200' },
    spam: { bg: 'bg-red-50', text: 'text-red-700', border: 'border-red-200' }
  };

  const updateStatus = async (contactId: string, newStatus: string) => {
    setProcessing(contactId);
    try {
      await updateDoc(doc(db, 'contacts', contactId), { status: newStatus });
      window.location.reload();
    } catch (error) {
      console.error('Error updating status:', error);
      alert('Failed to update status');
    } finally {
      setProcessing(null);
    }
  };

  const deleteContact = async (contactId: string) => {
    if (!confirm('Are you sure you want to delete this contact?')) return;
    
    setProcessing(contactId);
    try {
      await deleteDoc(doc(db, 'contacts', contactId));
      window.location.reload();
    } catch (error) {
      console.error('Error deleting contact:', error);
      alert('Failed to delete contact');
    } finally {
      setProcessing(null);
    }
  };

  const formatDate = (timestamp: any) => {
    if (!timestamp) return 'N/A';
    const date = timestamp.toDate ? timestamp.toDate() : new Date(timestamp);
    return new Intl.DateTimeFormat('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    }).format(date);
  };

  const filteredContacts = contacts.filter(contact => {
    const matchesFilter = filter === 'all' || contact.status === filter;
    const matchesSearch = 
      contact.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
      contact.email.toLowerCase().includes(searchTerm.toLowerCase()) ||
      contact.message.toLowerCase().includes(searchTerm.toLowerCase());
    return matchesFilter && matchesSearch;
  });

  const statusCounts = {
    all: contacts.length,
    new: contacts.filter(c => c.status === 'new' || !c.status).length,
    pending: contacts.filter(c => c.status === 'pending').length,
    resolved: contacts.filter(c => c.status === 'resolved').length,
    spam: contacts.filter(c => c.status === 'spam').length
  };

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-gray-800">Contact Messages</h1>
          <p className="text-gray-600 mt-1">Manage and respond to user inquiries</p>
        </div>
        <div className="bg-gradient-to-r from-pink-600 to-purple-600 text-white px-6 py-3 rounded-lg shadow-md">
          <p className="text-sm font-medium">Total Contacts</p>
          <p className="text-2xl font-bold">{contacts.length}</p>
        </div>
      </div>

      {/* Filters */}
      <div className="bg-white rounded-xl shadow-md p-6">
        <div className="flex flex-col md:flex-row gap-4">
          <div className="flex-1">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-5 h-5" />
              <input
                type="text"
                placeholder="Search by name, email, or message..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="w-full pl-10 pr-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-pink-400 focus:border-transparent"
              />
            </div>
          </div>
          <div className="flex gap-2 flex-wrap">
            {Object.entries(statusCounts).map(([status, count]) => (
              <button
                key={status}
                onClick={() => setFilter(status)}
                className={`px-4 py-2 rounded-lg font-medium transition-all ${
                  filter === status
                    ? 'bg-gradient-to-r from-pink-600 to-purple-600 text-white shadow-md'
                    : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                {status.charAt(0).toUpperCase() + status.slice(1)} ({count})
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Contacts List */}
      <div className="space-y-4">
        {filteredContacts.length === 0 ? (
          <div className="bg-white rounded-xl shadow-md p-12 text-center">
            <Mail className="w-16 h-16 text-gray-300 mx-auto mb-4" />
            <p className="text-gray-500 text-lg">No contacts found</p>
          </div>
        ) : (
          filteredContacts.map((contact) => {
            const status = contact.status || 'new';
            const colors = statusColors[status];
            const isExpanded = expandedContact === contact.uid;

            return (
              <div
                key={contact.uid}
                className="bg-white rounded-xl shadow-md overflow-hidden border-l-4 hover:shadow-lg transition-shadow"
                style={{ borderLeftColor: colors.border.replace('border-', '#') }}
              >
                <div className="p-6">
                  <div className="flex justify-between items-start mb-4">
                    <div className="flex-1">
                      <div className="flex items-center gap-3 mb-2">
                        <h3 className="text-xl font-bold text-gray-800">{contact.name}</h3>
                        <span className={`px-3 py-1 rounded-full text-xs font-semibold ${colors.bg} ${colors.text}`}>
                          {status.toUpperCase()}
                        </span>
                      </div>
                      <div className="flex flex-wrap gap-4 text-sm text-gray-600">
                        <div className="flex items-center gap-2">
                          <Mail className="w-4 h-4" />
                          <a href={`mailto:${contact.email}`} className="hover:text-pink-600">
                            {contact.email}
                          </a>
                        </div>
                        <div className="flex items-center gap-2">
                          <Tag className="w-4 h-4" />
                          <span>{subjectLabels[contact.subject] || contact.subject}</span>
                        </div>
                        <div className="flex items-center gap-2">
                          <Calendar className="w-4 h-4" />
                          <span>{formatDate(contact.createdAt)}</span>
                        </div>
                      </div>
                    </div>
                  </div>

                  {/* Message Preview/Full */}
                  <div className="mb-4">
                    <div className="flex items-start gap-2 mb-2">
                      <MessageSquare className="w-4 h-4 text-gray-400 mt-1" />
                      <p className="text-gray-700 flex-1">
                        {isExpanded ? contact.message : `${contact.message.substring(0, 150)}${contact.message.length > 150 ? '...' : ''}`}
                      </p>
                    </div>
                    {contact.message.length > 150 && (
                      <button
                        onClick={() => setExpandedContact(isExpanded ? null : contact.uid)}
                        className="text-pink-600 hover:text-pink-700 text-sm font-medium"
                      >
                        {isExpanded ? 'Show less' : 'Read more'}
                      </button>
                    )}
                  </div>

                  {/* Actions */}
                  <div className="flex flex-wrap gap-2 pt-4 border-t border-gray-100">
                    <button
                      onClick={() => updateStatus(contact.uid, 'resolved')}
                      disabled={processing === contact.uid || status === 'resolved'}
                      className="flex items-center gap-2 px-4 py-2 bg-green-50 text-green-700 rounded-lg hover:bg-green-100 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      <Check className="w-4 h-4" />
                      Mark Resolved
                    </button>
                    <button
                      onClick={() => updateStatus(contact.uid, 'pending')}
                      disabled={processing === contact.uid || status === 'pending'}
                      className="flex items-center gap-2 px-4 py-2 bg-yellow-50 text-yellow-700 rounded-lg hover:bg-yellow-100 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      <Filter className="w-4 h-4" />
                      Mark Pending
                    </button>
                    <button
                      onClick={() => updateStatus(contact.uid, 'spam')}
                      disabled={processing === contact.uid || status === 'spam'}
                      className="flex items-center gap-2 px-4 py-2 bg-red-50 text-red-700 rounded-lg hover:bg-red-100 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      <X className="w-4 h-4" />
                      Mark Spam
                    </button>
                    <button
                      onClick={() => deleteContact(contact.uid)}
                      disabled={processing === contact.uid}
                      className="flex items-center gap-2 px-4 py-2 bg-gray-50 text-gray-700 rounded-lg hover:bg-gray-100 transition-colors disabled:opacity-50 disabled:cursor-not-allowed ml-auto"
                    >
                      <Trash2 className="w-4 h-4" />
                      Delete
                    </button>
                  </div>
                </div>
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}