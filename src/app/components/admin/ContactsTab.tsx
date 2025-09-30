'use client';

interface ContactData {
  uid: string;
  name: string;
  email: string;
  message: string;
  createdAt: any;
}

interface ContactsTabProps {
  contacts: ContactData[];
}

export default function ContactsTab({ contacts }: ContactsTabProps) {
  return (
    <section>
      <h2 className="text-2xl font-bold text-pink-600 mb-6">Contact Messages</h2>
      {contacts.length === 0 ? (
        <p className="text-gray-500">No contact messages yet.</p>
      ) : (
        <div className="space-y-4">
          {contacts.map(contact => (
            <div key={contact.uid} className="p-4 border rounded bg-white">
              <div className="flex justify-between items-start mb-2">
                <div className="flex-1">
                  <p className="font-semibold">{contact.name}</p>
                  <p className="text-sm text-gray-600">{contact.email}</p>
                  <p className="text-gray-700 mt-2">{contact.message}</p>
                  <p className="text-xs text-gray-500 mt-2">
                    {contact.createdAt?.toDate ? 
                      contact.createdAt.toDate().toLocaleString() : 
                      new Date(contact.createdAt).toLocaleString()
                    }
                  </p>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}