'use client';

import { updateDoc, doc, deleteDoc } from 'firebase/firestore';
import { db } from '@/lib/firebase';

interface UserQuestion {
  id: string;
  question: string;
  userEmail: string;
  userName: string;
  answered: boolean;
  answer?: string;
  createdAt: any;
  answeredAt?: any;
}

interface UserQuestionsTabProps {
  userQuestions: UserQuestion[];
  fetchUserQuestions: () => void;
  setNewFAQForm: React.Dispatch<React.SetStateAction<any>>;
  setActiveTab: (tab: string) => void;
}

export default function UserQuestionsTab({ 
  userQuestions, 
  fetchUserQuestions, 
  setNewFAQForm, 
  setActiveTab 
}: UserQuestionsTabProps) {
  const handleAnswerUserQuestion = async (questionId: string, answer: string) => {
    await updateDoc(doc(db, "user-questions", questionId), {
      answer: answer.trim(),
      answered: true,
      answeredAt: new Date()
    });
    fetchUserQuestions();
  };

  const handleDeleteUserQuestion = async (id: string) => {
    if (!confirm("Are you sure you want to delete this question?")) return;
    await deleteDoc(doc(db, "user-questions", id));
    fetchUserQuestions();
  };

  return (
    <section>
      <h2 className="text-2xl font-bold text-pink-600 mb-6">User Questions</h2>

      {userQuestions.length === 0 ? (
        <p className="text-gray-500">No user questions yet.</p>
      ) : (
        <div className="space-y-4">
          {userQuestions.map(question => (
            <div
              key={question.id}
              className={`p-4 border rounded ${
                question.answered ? 'bg-green-50 border-green-200' : 'bg-yellow-50 border-yellow-200'
              }`}
            >
              <div className="flex justify-between items-start mb-3">
                <div className="flex-1">
                  <p className="font-semibold text-gray-800">{question.question}</p>
                  <p className="text-sm text-gray-600 mt-1">
                    Asked by: <span className="font-medium">{question.userName}</span> ({question.userEmail})
                  </p>
                  <p className="text-xs text-gray-500">
                    {question.createdAt?.toDate ?
                      question.createdAt.toDate().toLocaleString() :
                      new Date(question.createdAt).toLocaleString()
                    }
                  </p>
                </div>

                <div className="flex items-center gap-2">
                  <span className={`px-2 py-1 text-xs rounded ${
                    question.answered ? 'bg-green-100 text-green-800' : 'bg-yellow-100 text-yellow-800'
                  }`}>
                    {question.answered ? 'Answered' : 'Pending'}
                  </span>
                  <button
                    onClick={() => handleDeleteUserQuestion(question.id)}
                    className="px-2 py-1 bg-red-500 text-white rounded text-xs"
                  >
                    Delete
                  </button>
                </div>
              </div>

              {question.answered ? (
                <div className="mt-3 p-3 bg-white rounded border">
                  <p className="text-sm font-medium text-green-700">Answer:</p>
                  <p className="text-sm text-gray-700 mt-1">{question.answer}</p>
                  <p className="text-xs text-gray-500 mt-2">
                    Answered on: {question.answeredAt?.toDate ?
                      question.answeredAt.toDate().toLocaleString() :
                      'Unknown'
                    }
                  </p>
                </div>
              ) : (
                <div className="mt-3">
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Provide Answer:
                  </label>
                  <div className="flex gap-2">
                    <input
                      type="text"
                      placeholder="Type your answer here..."
                      className="flex-1 p-2 border border-gray-300 rounded text-sm"
                      onKeyPress={(e) => {
                        if (e.key === 'Enter' && e.currentTarget.value.trim()) {
                          handleAnswerUserQuestion(question.id, e.currentTarget.value);
                          e.currentTarget.value = '';
                        }
                      }}
                    />
                    <button
                      onClick={(e) => {
                        const input = e.currentTarget.previousElementSibling as HTMLInputElement;
                        if (input.value.trim()) {
                          handleAnswerUserQuestion(question.id, input.value);
                          input.value = '';
                        }
                      }}
                      className="px-4 py-2 bg-green-500 text-white rounded text-sm hover:bg-green-600"
                    >
                      Answer
                    </button>
                  </div>

                  {/* Convert to FAQ option */}
                  <button
                    onClick={() => {
                      setNewFAQForm({
                        question: question.question,
                        answer: '',
                        category: 'general'
                      });
                      setActiveTab('FAQs');
                    }}
                    className="mt-2 px-3 py-1 bg-blue-500 text-white rounded text-xs hover:bg-blue-600"
                  >
                    Convert to FAQ
                  </button>
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Statistics */}
      <div className="mt-8 grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="p-4 bg-white border rounded shadow text-center">
          <p className="text-gray-500">Total Questions</p>
          <p className="text-2xl font-bold text-pink-600">{userQuestions.length}</p>
        </div>
        <div className="p-4 bg-white border rounded shadow text-center">
          <p className="text-gray-500">Answered</p>
          <p className="text-2xl font-bold text-green-600">
            {userQuestions.filter(q => q.answered).length}
          </p>
        </div>
        <div className="p-4 bg-white border rounded shadow text-center">
          <p className="text-gray-500">Pending</p>
          <p className="text-2xl font-bold text-yellow-600">
            {userQuestions.filter(q => !q.answered).length}
          </p>
        </div>
      </div>
    </section>
  );
}