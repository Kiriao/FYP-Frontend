import type { Metadata } from "next";
import Script from "next/script";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";
import Navbar from "./components/Navbar";
import Footer from "./components/Footer";
import DialogflowMessenger from "@/app/components/DialogflowMessenger";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "KidFlix",
  description: "Discover books and educational videos for kids",
};

// export default function RootLayout({ children }: { children: React.ReactNode }) {
//   return (
//     <html lang="en">
//       <head>
//         <Script
//           src="https://www.gstatic.com/dialogflow-console/fast/messenger/bootstrap.js?v=1"
//           strategy="afterInteractive"
//         />
//       </head>
//       <body className={`${geistSans.variable} ${geistMono.variable} antialiased`}>
//         <Navbar />
//         <main>{children}</main>
//         <Footer />
        
        
//       </body>
//     </html>
//   );
// }
export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
            <head>
        <Script
          src="https://www.gstatic.com/dialogflow-console/fast/messenger/bootstrap.js?v=1"
          strategy="afterInteractive"
        />
      </head>
      {/* keep <head> minimal; the component will load the script */}
      <body className={`${geistSans.variable} ${geistMono.variable} antialiased`}>
        <Navbar />
        <main>{children}</main>
        <Footer />

        {/* Mount the bot ONCE for the whole app */}
        <DialogflowMessenger />
      </body>
    </html>
  );
}

