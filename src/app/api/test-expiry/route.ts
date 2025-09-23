// app/api/test-expiry/route.ts
import { NextResponse } from 'next/server';

export async function GET() {
  try {
    const baseUrl = process.env.VERCEL_URL 
      ? `https://${process.env.VERCEL_URL}` 
      : process.env.NEXTAUTH_URL 
      ? process.env.NEXTAUTH_URL 
      : 'http://localhost:3000';

    console.log('🧪 Testing expiry check manually...');
    
    const response = await fetch(`${baseUrl}/api/check-expired-plans`, {
      headers: {
        'Authorization': `Bearer ${process.env.CRON_SECRET_KEY}`
      }
    });
    
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }
    
    const result = await response.json();
    
    return NextResponse.json({
      test: 'Manual expiry check completed',
      result: result,
      timestamp: new Date().toISOString()
    });
  } catch (error: any) {
    console.error('❌ Test failed:', error);
    return NextResponse.json({ 
      error: 'Test failed', 
      message: error.message,
      timestamp: new Date().toISOString()
    }, { status: 500 });
  }
}