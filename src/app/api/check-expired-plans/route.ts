// app/api/check-expired-plans/route.ts (App Router)
// OR pages/api/check-expired-plans.ts (Pages Router)

import { NextRequest, NextResponse } from 'next/server';
import { db } from '@/lib/firebase';
import { 
  collection, 
  query, 
  where, 
  getDocs, 
  writeBatch,
  doc 
} from 'firebase/firestore';

// Helper function to check if plan is expired
const isPlanExpired = (expirationDate: any): boolean => {
  if (!expirationDate) return true;
  const expDate = expirationDate.toDate();
  return new Date() > expDate;
};

export async function GET(request: NextRequest) {
  try {
    // ✅ Security: Check for API key
    const authHeader = request.headers.get('authorization');
    const apiKey = process.env.CRON_SECRET_KEY;
    
    if (!apiKey || authHeader !== `Bearer ${apiKey}`) {
      console.log('❌ Unauthorized cron request');
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }

    console.log('🕐 Starting scheduled plan expiry check...');

    // Get all users with active plans
    const usersRef = collection(db, 'users');
    const activeUsersQuery = query(usersRef, where('isActive', '==', true));
    const snapshot = await getDocs(activeUsersQuery);

    if (snapshot.empty) {
      console.log('✅ No active users found');
      return NextResponse.json({ 
        success: true, 
        message: 'No active users found',
        expired: 0,
        soonToExpire: 0,
        timestamp: new Date().toISOString()
      });
    }

    const batch = writeBatch(db);
    let expiredCount = 0;
    let soonToExpireCount = 0;
    const expiredUsers = [];
    const soonToExpireUsers = [];

    for (const docSnap of snapshot.docs) {
      const userData = docSnap.data();
      const planExpiresAt = userData.planExpiresAt;
      
      // Skip free plans or users without expiry date
      if (!planExpiresAt || userData.plan === 'Free Plan') {
        continue;
      }

      const expDate = planExpiresAt.toDate();
      const now = new Date();
      const daysUntilExpiry = Math.ceil((expDate.getTime() - now.getTime()) / (1000 * 60 * 60 * 24));

      // Check if plan has expired
      if (isPlanExpired(planExpiresAt)) {
        console.log(`⏰ Expiring plan for user ${docSnap.id}: ${userData.plan}`);
        
        const userRef = doc(db, 'users', docSnap.id);
        batch.update(userRef, {
          plan: 'Free Plan',
          isActive: false,
          planExpiredAt: now,
          previousPlan: userData.plan,
        });

        // Update subscription records
        const subscriptionsQuery = query(
          collection(db, 'subscriptions'),
          where('userId', '==', docSnap.id),
          where('isActive', '==', true)
        );
        
        const subsSnapshot = await getDocs(subscriptionsQuery);
        subsSnapshot.docs.forEach(subDoc => {
          batch.update(subDoc.ref, {
            isActive: false,
            expiredAt: now,
          });
        });

        expiredUsers.push({
          userId: docSnap.id,
          previousPlan: userData.plan,
          email: userData.email || 'no-email'
        });
        expiredCount++;
      } 
      // Check if plan expires soon (within 3 days)
      else if (daysUntilExpiry <= 3 && daysUntilExpiry > 0) {
        console.log(`⚠️ Plan expires soon for user ${docSnap.id}: ${daysUntilExpiry} days`);
        
        const userRef = doc(db, 'users', docSnap.id);
        batch.update(userRef, {
          expiryWarningShown: true,
          daysUntilExpiry: daysUntilExpiry,
          lastWarningAt: now,
        });
        
        soonToExpireUsers.push({
          userId: docSnap.id,
          plan: userData.plan,
          daysLeft: daysUntilExpiry,
          email: userData.email || 'no-email'
        });
        soonToExpireCount++;
      }
    }

    // Commit all updates
    if (expiredCount > 0 || soonToExpireCount > 0) {
      await batch.commit();
      console.log(`✅ Batch update completed:`);
      console.log(`   - Expired ${expiredCount} plans`);
      console.log(`   - ${soonToExpireCount} plans expiring soon`);
    } else {
      console.log('✅ No plans need updating');
    }

    return NextResponse.json({
      success: true,
      message: 'Plan expiry check completed successfully',
      stats: {
        totalChecked: snapshot.docs.length,
        expired: expiredCount,
        soonToExpire: soonToExpireCount,
      },
      details: {
        expiredUsers: expiredUsers,
        soonToExpireUsers: soonToExpireUsers,
      },
      timestamp: new Date().toISOString()
    });

  } catch (error: any) {
    console.error('❌ Error checking expired plans:', error);
    return NextResponse.json(
      { 
        error: 'Failed to check expired plans', 
        message: error.message,
        timestamp: new Date().toISOString()
      },
      { status: 500 }
    );
  }
} 