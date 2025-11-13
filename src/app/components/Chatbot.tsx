// "use client";

// import { useState } from "react";
// import { Send, MessageCircle, X } from "lucide-react";

// export default function Chatbot() {
//   const [open, setOpen] = useState(false);
//   const [messages, setMessages] = useState<{ role: string; content: string }[]>([
//     { role: "bot", content: "Hi 👋 I can recommend books and videos for kids! What are you looking for?" }
//   ]);
//   const [input, setInput] = useState("");

//   const sendMessage = async () => {
//     if (!input.trim()) return;

//     // Add user message
//     const newMessages = [...messages, { role: "user", content: input }];
//     setMessages(newMessages);
//     setInput("");

//     // Call your API route
//     const res = await fetch("/api/chat", {
//       method: "POST",
//       headers: { "Content-Type": "application/json" },
//       body: JSON.stringify({ messages: newMessages })
//     });

//     const data = await res.json();
//     setMessages([...newMessages, { role: "bot", content: data.reply }]);
//   };

//   return (
//     <div className="fixed bottom-4 right-4">
//       {/* Floating Icon */}
//       {!open && (
//         <button
//           onClick={() => setOpen(true)}
//           className="p-4 bg-blue-500 text-white rounded-full shadow-lg hover:bg-blue-600 transition"
//         >
//           <MessageCircle size={24} />
//         </button>
//       )}

//       {/* Chat Window */}
//       {open && (
//         <div className="w-80 h-96 bg-white shadow-lg rounded-2xl p-4 flex flex-col text-[#111]">
//           {/* Header */}
//           <div className="flex justify-between items-center border-b pb-2 mb-2">
//             <h2 className="font-bold">Chatbot</h2>
//             <button
//               onClick={() => setOpen(false)}
//               className="p-1 hover:bg-gray-100 rounded-full"
//             >
//               <X size={20} />
//             </button>
//           </div>

//           {/* Messages */}
//           <div className="flex-1 overflow-y-auto space-y-2">
//             {messages.map((m, i) => (
//               <div
//                 key={i}
//                 className={`p-2 rounded-lg max-w-[75%] ${
//                   m.role === "bot"
//                     ? "bg-gray-200 self-start"
//                     : "bg-blue-500 text-white self-end"
//                 }`}
//               >
//                 {m.content}
//               </div>
//             ))}
//           </div>

//           {/* Input */}
//           <div className="flex mt-2">
//             <input
//               className="flex-1 border rounded-lg p-2"
//               value={input}
//               onChange={(e) => setInput(e.target.value)}
//               onKeyDown={(e) => e.key === "Enter" && sendMessage()}
//               placeholder="Ask me about books or videos..."
//             />
//             <button
//               onClick={sendMessage}
//               className="ml-2 p-2 bg-blue-500 text-white rounded-lg"
//             >
//               <Send size={18} />
//             </button>
//           </div>
//         </div>
//       )}
//     </div>
//   );
// }
